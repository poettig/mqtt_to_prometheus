#!/usr/bin/env python3
import abc
import argparse
import base64
import json
import logging
import math
import pathlib
import re
import signal
import sys
import threading
import time
import traceback
import types
from collections.abc import Generator
from typing import Any, ClassVar

import paho.mqtt.client as mqtt
import prometheus_client
from prometheus_client import Counter, Gauge
from pydantic import Json

prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)

Rule = str
Labels = dict[str, str]

LOGGING_LEVEL_TRACE = 9


class Filter:
    def __init__(self, rules: list[Rule], max_dropped_values: int) -> None:
        self.rules: list[Rule] = rules
        self.max_dropped_values = max_dropped_values
        self.already_dropped_values = 0


def setup_logging(quiet: bool, debug: bool, trace: bool, timestamps: bool) -> None:
    log_date_format = "%Y-%m-%d %H:%M:%S"
    log_format = "%(levelname)8s: %(message)s"

    if debug or trace:
        log_level = LOGGING_LEVEL_TRACE if trace else logging.DEBUG
        log_format = "[%(name)s] " + log_format
    elif quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    if timestamps:
        log_format = "[%(asctime)s] " + log_format

    logging.addLevelName(LOGGING_LEVEL_TRACE, "TRACE")
    logging.basicConfig(level=log_level, format=log_format, datefmt=log_date_format)


def extract_labels_from_topic_segments(labels_data: list[str]) -> dict[Any, Any]:
    if len(labels_data) % 2 != 0:
        raise ValueError(f"Labels extracted from topic are not an even number of elements: {labels_data}")

    # Split labels data into keys and values
    labels = {}
    labels_data_iterator = iter(labels_data)
    for key in labels_data_iterator:
        labels[key] = next(labels_data_iterator)
    return labels


# https://stackoverflow.com/a/1176023/6371499
class CamelToSnakeConverter:
    any_char_followed_by_uppercase_letter_pattern = re.compile(r"([^_])([A-Z][a-z]+)")
    lower_or_number_followed_by_uppercase_letter_pattern = re.compile(r"([a-z0-9])([A-Z])")

    @staticmethod
    def convert(string: str) -> str:
        result = re.sub("-", "_", string)
        result = CamelToSnakeConverter.any_char_followed_by_uppercase_letter_pattern.sub(r"\1_\2", result)
        result = CamelToSnakeConverter.lower_or_number_followed_by_uppercase_letter_pattern.sub(r"\1_\2", result)
        return result.lower()


class ThreadedManager(abc.ABC):
    def __init__(self, name: str, interval: float) -> None:
        self.exception: BaseException | None = None

        self._interval = interval
        self._running = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.name = name

    def _run(self) -> None:
        self.prepare()

        start = 0
        while self._running:
            if self._interval > 1:
                if time.time() - start < self._interval:
                    # Wait in small segments to not block an exit request forever
                    time.sleep(1)
                    continue
            else:
                time.sleep(self._interval)

            # Run iteration
            logging.log(LOGGING_LEVEL_TRACE, f"Run iteration loop of thread {self._thread.name}")
            self.run_iteration()

            # Reset sleep time
            start = time.time()

        self.teardown()

    def start(self) -> None:
        logging.debug(f"Starting thread {self._thread.name}")
        self._running = True
        self._thread.start()

    def stop(self) -> None:
        logging.debug(f"Stopping thread {self._thread.name}")
        self._running = False
        self._thread.join()

    def prepare(self) -> None:  # noqa: B027
        # Does nothing unless overwritten on purpose
        pass

    @abc.abstractmethod
    def run_iteration(self) -> None:
        raise NotImplementedError

    def teardown(self) -> None:  # noqa: B027
        # Does nothing unless overwritten on purpose
        pass


class MetricWithChangeTracking(abc.ABC):
    _prefix: ClassVar[str]

    def __init__(self, name: str, labels: Labels, is_counter: bool = False, documentation: str = "") -> None:
        initializer = Counter if is_counter else Gauge

        self._name = name
        self._labels = labels
        self.last_set: float = 0
        self._metric = initializer(self.name, documentation, self._labels.keys()).labels(*self._labels.values())

    @staticmethod
    def create_metric_identifier(name: str, labels: Labels) -> str:
        # Using sorted(labels.items()) is fine here as values are always strings
        labelset_as_string = [key + "=" + value for key, value in sorted(labels.items())]
        return f"{name}{{{', '.join(labelset_as_string)}}}"

    @property
    def name(self) -> str:
        return self._prefix + self._name

    @property
    def full_name(self) -> str:
        return self.create_metric_identifier(self._name, self._labels)

    @property
    def value(self) -> float:
        return self._metric._value.get()

    def __str__(self) -> str:
        if self.last_set == 0:
            return f"{self.full_name} <currently not set>"

        return f"{self.full_name} {self.value}"

    def inc(self, amount: float = 1.0) -> None:
        self._metric.inc(amount)
        self.last_set = time.time()
        logging.debug(f"Increased {self.full_name} by {amount} to {self.value}")

    def dec(self, amount: float = 1.0) -> None:
        if isinstance(self._metric, Counter):
            raise ValueError(f"Tried to decrease counter metric {self.full_name}")

        self._metric.dec(amount)
        self.last_set = time.time()
        logging.debug(f"Decreased {self.full_name} by {amount} to {self.value}")

    def set(self, value: float) -> None:
        if isinstance(self._metric, Counter):
            raise ValueError(f"Tried to set counter metric {self.full_name}")

        self._metric.set(value)
        self.last_set = time.time()
        logging.debug(f"Set {self.full_name} to {self.value}")

    def remove(self) -> bool:
        if self.last_set == 0:
            # Currently not set, removing makes no sense
            logging.debug(f"Tried to remove {self.full_name} but it is currently not set")
            return False

        self._metric.remove(*self._labels.values())
        self.last_set = 0
        logging.debug(f"Removed {self.full_name}")

        return True


class TasmotaMetricWithChangeTracking(MetricWithChangeTracking):
    _prefix = "tasmota_"


class ShellyMetricWithChangeTracking(MetricWithChangeTracking):
    _prefix = "shelly_"


class FaikoutMetricWithChangeTracking(MetricWithChangeTracking):
    _prefix = "faikout_"


class Zigbee2MQTTMetricWithChangeTracking(MetricWithChangeTracking):
    _prefix = "zigbee2mqtt_"


class NoPrefixMetricWithChangeTracking(MetricWithChangeTracking):
    _prefix = ""


class MetricsManager(ThreadedManager, abc.ABC):
    _last_update_gauge: Gauge | None = None
    _last_update_gauge_lock: threading.Lock = threading.Lock()

    def __init__(
        self,
        filters: dict,
        cleanup_interval: int,
        cleanup_threshold: int,
    ) -> None:
        self._metrics: dict[str, MetricWithChangeTracking] = {}
        self._filters: dict[str, Filter] = {}
        self._cleanup_threshold = cleanup_threshold

        for entry in filters:
            for metric_name in entry["metric_names"]:
                self._filters[metric_name] = Filter(
                    rules=entry["rules"],
                    max_dropped_values=entry["max_dropped_values"],
                )

        super().__init__(self.__class__.__name__, cleanup_interval)

    @property
    @abc.abstractmethod
    def _metric_type(self) -> type[MetricWithChangeTracking]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def mqtt_subscribe_prefix(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def _extract_labels(self, topic: str) -> tuple[Labels, str] | None:
        """
        Extracts labels for metrics from the given topic.

        :param topic: The topic to extract labels from.

        :returns: A labelset consisting of key-value-pairs as a dict AND
                  the part of the topic that was not consumed into labels OR
                  None if it was not possible to parse any labels
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def _extract_metrics(remaining_topic: str, json_data: Json) -> list[tuple[str, float]]:
        """
        Extracts metrics data from the remaining topic after parsing labels and the JSON data of the message.

        :param remaining_topic: The remaining topic after labels were extracted
        :param json_data: The JSON data from the MQTT message

        :returns: A list of tuples where each tuple contains a metric name and its value
        """
        raise NotImplementedError

    @staticmethod
    def _create_metric_with_value(
        metric_name: str,
        value: float,
        metric_prefix: str | None = None,
    ) -> tuple[str, int | float]:
        full_metric_name = metric_name
        if metric_prefix:
            full_metric_name = f"{metric_prefix}_{metric_name}"

        return full_metric_name, value

    @staticmethod
    def _recursive_metrics_generator(json_data: Json, prefix: str | None = None) -> Generator[tuple[str, float]]:
        if isinstance(json_data, list):
            # Extract metrics for each list entry
            for entry in json_data:
                yield from MetricsManager._recursive_metrics_generator(entry)

        elif isinstance(json_data, dict):
            # Iterate whole dict
            for key, value in json_data.items():
                if isinstance(value, list):
                    # Iterate list and check which value type it is
                    for idx, entry in enumerate(value):
                        if isinstance(entry, int | float):
                            # For lists of numbers, add an identifier if there is more than one entry
                            metric_name = key
                            if len(value) > 1:
                                metric_name = f"{key}{idx + 1}"

                            yield MetricsManager._create_metric_with_value(metric_name, entry, prefix)

                        elif isinstance(entry, dict):
                            yield from MetricsManager._recursive_metrics_generator(entry, prefix=key)

                        # Lists in lists are ignored

                elif isinstance(value, list | dict):
                    yield from MetricsManager._recursive_metrics_generator(value, prefix=key)

                # Generate metric if the value is a number
                elif isinstance(value, int | float):
                    yield MetricsManager._create_metric_with_value(key, value, prefix)

    def _get_metric(
        self,
        metric_name: str,
        labels: dict[str, str],
        is_counter: bool = False,
        documentation: str = "",
    ) -> MetricWithChangeTracking:
        identifier = MetricWithChangeTracking.create_metric_identifier(metric_name, labels)
        if identifier in self._metrics:
            # Use already stored metric from cache
            return self._metrics[identifier]

        # Create new metric and store
        metric = self._metric_type(
            CamelToSnakeConverter.convert(metric_name),
            labels,
            is_counter=is_counter,
            documentation=documentation,
        )
        self._metrics[identifier] = metric
        return metric

    def should_be_filtered(self, metric: MetricWithChangeTracking, new_value: float) -> bool:
        filter_info = self._filters.get(metric.name)
        if filter_info is None:
            # No filter defined
            return False

        filter_messages = []
        for rule in filter_info.rules:
            rule_type, rule_value = rule.split(":")
            rule_value = float(rule_value)

            if rule_type == "diff":
                # Don't apply diff filter if the metric was never set before
                if metric.last_set != 0 and abs(new_value - metric.value) > rule_value:
                    filter_messages.append(
                        f"{new_value} differs from previous value {metric.value} by more than {rule_value}."
                    )

            elif rule_type == "above":
                if new_value > rule_value:
                    filter_messages.append(f"{new_value} is above {rule_value}.")

            elif rule_type == "below":
                if new_value < rule_value:
                    filter_messages.append(f"{new_value} is below {rule_value}.")

            elif rule_type == "value":
                if math.isclose(new_value, rule_value):
                    filter_messages.append(f"{new_value} is a forbidden value.")

            else:
                raise ValueError(f"Invalid rule type '{rule_type}'.")

        # No filter was hit
        if len(filter_messages) == 0:
            if filter_info.already_dropped_values != 0:
                filter_info.already_dropped_values = 0
                logging.info(f"No filter hit for {metric}, reset drop counter.")

            return False

        filter_info.already_dropped_values += 1
        if filter_info.already_dropped_values >= filter_info.max_dropped_values:
            # Always accept when the maximum dropped values are reached
            filter_info.already_dropped_values = 0
            logging.warning(
                f"Accepted filtered value {metric} because maximum drops are reached. Drop reasons would have been:"
            )
            for filter_message in filter_messages:
                logging.warning(filter_message)

            return False

        logging.warning(
            f"[{filter_info.already_dropped_values}/{filter_info.max_dropped_values}]"
            f" Filtered new value for {metric.full_name}. Reasons:"
        )
        for filter_message in filter_messages:
            logging.warning(filter_message)

        return True

    def process_mqtt_message(self, topic: str, json_data: Json) -> None:
        # Extract labelset from topic
        result = self._extract_labels(topic)
        if not result:
            logging.debug(f"Could not extract labels from {topic}")
            return

        labels, remaining_topic = result

        # Extract metrics from remaining topic elements and json_data
        metrics_data = self._extract_metrics(remaining_topic, json_data)
        if not metrics_data:
            logging.debug(f"Could not extract any metric from {remaining_topic} and {json_data}")
            return

        message_counter = self._get_metric(
            "processed_messages",
            labels,
            is_counter=True,
            documentation="Number of MQTT messages processed for this topic.",
        )
        drop_counter = self._get_metric(
            "dropped_values",
            labels,
            is_counter=True,
            documentation="Number of metric values dropped because of a filter hit.",
        )

        message_counter.inc()
        for metric_name, value in metrics_data:
            metric = self._get_metric(metric_name, labels)

            # Drop update if filtered, else set the gauge to the new value
            if self.should_be_filtered(metric, value):
                drop_counter.inc()
            else:
                metric.set(value)

        # Update "last received" metric for topic which also is never filtered

        # Prepare labels
        last_update_labels = {
            **labels,
            "topic": remaining_topic,
            "type": re.sub(r"_$", "", self._metric_type._prefix),
        }

        # Create last update gauge metric if not done yet
        if MetricsManager._last_update_gauge is None:
            with MetricsManager._last_update_gauge_lock:
                # In case multiple threads passed the outer check and started waiting for a lock,
                # ensure that only one thread creates the metric
                if MetricsManager._last_update_gauge is None:
                    MetricsManager._last_update_gauge = Gauge(
                        "last_update",
                        "Last update of metric with a specific labelset",
                        last_update_labels.keys(),
                    )

        # Set last update metric value
        assert MetricsManager._last_update_gauge is not None
        MetricsManager._last_update_gauge.labels(*last_update_labels.values()).set(time.time())

    def run_iteration(self) -> None:
        logging.debug("Running metrics cleanup...")

        for metric in self._metrics.values():
            if time.time() - metric.last_set > self._cleanup_threshold and metric.remove():
                logging.info(f"Removed metric {metric.full_name} as it was inactive for {self._cleanup_threshold}s.")


class TasmotaMetricsManager(MetricsManager):
    message_types_to_parse = (
        "STATE",
        "SENSOR",
    )

    def _extract_labels(self, topic: str) -> tuple[Labels, str] | None:
        topic_elements = topic.split("/")
        if topic_elements[-1] not in TasmotaMetricsManager.message_types_to_parse:
            return None

        # Remove the last topic element as that is a part of the metric name, not a label
        metric_labels = extract_labels_from_topic_segments(topic_elements[:-1])
        return metric_labels, topic_elements[-1]

    @staticmethod
    def _extract_metrics(remaining_topic: str, json_data: Json) -> list[tuple[str, float]]:
        return list(MetricsManager._recursive_metrics_generator(json_data))

    @property
    def _metric_type(self) -> type[MetricWithChangeTracking]:
        return TasmotaMetricWithChangeTracking

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "tele"


class ShellyMetricsManager(MetricsManager):
    message_types_to_parse = ("status",)

    def _extract_labels(self, topic: str) -> tuple[Labels, str] | None:
        found_message_type = None
        for message_type in self.message_types_to_parse:
            if f"/{message_type}" in topic:
                found_message_type = message_type
                break

        if found_message_type is None:
            return None

        topic_elements = topic.split("/")

        # Split labels data into keys and values
        metric_labels = {}
        metric_labels_data_iterator = iter(topic_elements)
        for key in metric_labels_data_iterator:
            if key == found_message_type:
                break

            try:
                value = next(metric_labels_data_iterator)
            except StopIteration:
                value = None

            if value is None or value == found_message_type:
                break

            metric_labels[key] = value

        return metric_labels, "/".join(metric_labels_data_iterator)

    @staticmethod
    def _extract_metrics(remaining_topic: str, json_data: Json) -> list[tuple[str, float]]:
        result = []
        for metric_name, value in MetricsManager._recursive_metrics_generator(json_data):
            metric_name_prefix = None
            if remaining_topic:
                # Only use the last segment for the metric name
                # Also replace colons with underscores
                metric_name_prefix = remaining_topic.split("/")[-1].replace(":", "_")

            full_metric_name = metric_name
            if metric_name_prefix:
                full_metric_name = f"{metric_name_prefix}_{metric_name}"

            result.append((full_metric_name, value))

        return result

    @property
    def _metric_type(self) -> type[MetricWithChangeTracking]:
        return ShellyMetricWithChangeTracking

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "shelly"


class FaikoutMetricsManager(MetricsManager):
    message_types_to_parse = ("status",)
    mode_mapping: ClassVar[dict[str, int]] = {"H": 1, "C": 2, "D": 3, "F": 4, "A": 5}
    fan_mapping: ClassVar[dict[str, int]] = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "A": 6, "Q": 7}

    def __init__(self, location: str, filters: dict, cleanup_interval: int, cleanup_threshold: int) -> None:
        super().__init__(filters, cleanup_interval, cleanup_threshold)
        self.location = location

    def _extract_labels(self, topic: str) -> tuple[Labels, str] | None:
        found_message_type = None
        for message_type in self.message_types_to_parse:
            if f"/{message_type}" in topic:
                found_message_type = message_type
                break

        if found_message_type is None:
            return None

        # Faikout does not allow custom topics, have to hardcode it
        topic_elements = topic.split("/")
        return {"location": self.location, "device": topic_elements[0]}, ""

    @staticmethod
    def _extract_metrics(remaining_topic: str, json_data: Json) -> list[tuple[str, float]]:
        # Convert mode and fan speed to a number
        if json_data.get("mode"):
            json_data["mode"] = FaikoutMetricsManager.mode_mapping[json_data["mode"]]
        if json_data.get("fan"):
            json_data["fan"] = FaikoutMetricsManager.fan_mapping[json_data["fan"]]

        result = []
        for metric_name, value in MetricsManager._recursive_metrics_generator(json_data):
            result.append((metric_name, value))

        return result

    @property
    def _metric_type(self) -> type[MetricWithChangeTracking]:
        return FaikoutMetricWithChangeTracking

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "Faikout"


class Zigbee2MQTTMetricsManager(MetricsManager):
    ignore_bridge_topic_pattern = re.compile(r"^.*/bridge(?:/.*)?$")

    def _extract_labels(self, topic: str) -> tuple[Labels, str] | None:
        # Ignore specific topic patterns
        if self.ignore_bridge_topic_pattern.fullmatch(topic):
            return None

        topic_elements = topic.split("/")

        # Ignore all messages that are not the device info itself
        # That means: everything with an uneven number of topic segments
        # e.g., ignore "location/house/device/thermometer/level", but allow "location/house/device/thermometer"
        if len(topic_elements) % 2 == 1:
            return None

        metric_labels = extract_labels_from_topic_segments(topic_elements)
        return metric_labels, ""

    @staticmethod
    def _extract_metrics(remaining_topic: str, json_data: Json) -> list[tuple[str, float]]:
        return list(MetricsManager._recursive_metrics_generator(json_data))

    @property
    def _metric_type(self) -> type[MetricWithChangeTracking]:
        return Zigbee2MQTTMetricWithChangeTracking

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "zigbee2mqtt"


class NoPrefixRawValuesManager(MetricsManager):
    @property
    def _metric_type(self) -> type[MetricWithChangeTracking]:
        return NoPrefixMetricWithChangeTracking

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "noprefixraw"

    def _extract_labels(self, topic: str) -> tuple[Labels, str] | None:
        topic_elements = topic.split("/")
        if len(topic_elements) % 2 == 0:
            raise ValueError(f"Topic leaves no metric name at the end: {topic_elements}")

        # Split labels data into keys and values
        metric_labels_data = topic_elements[:-1]
        metric_labels = {}
        metric_labels_data_iterator = iter(metric_labels_data)
        for key in metric_labels_data_iterator:
            metric_labels[key] = next(metric_labels_data_iterator)

        return metric_labels, topic_elements[-1]

    @staticmethod
    def _extract_metrics(remaining_topic: str, json_data: Json) -> list[tuple[str, float]]:
        if not isinstance(json_data, str | float | int):
            raise ValueError(f"Can't extract metric with non-float input {type(json_data)}")

        return [(remaining_topic, float(json_data))]


class MQTTManager(ThreadedManager):
    def __init__(
        self,
        metrics_managers: list[MetricsManager],
        host: str,
        port: int,
        user: str,
        password: str,
        tls: bool,
    ) -> None:
        self._metrics_managers = {}
        for metrics_manager in metrics_managers:
            self._metrics_managers[metrics_manager.mqtt_subscribe_prefix] = metrics_manager

        self._mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_disconnect = self.on_disconnect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.on_log = self.on_log
        self._mqtt_client.username_pw_set(user, password)
        if tls:
            self._mqtt_client.tls_set_context()

        self._host = host
        self._port = port

        super().__init__(self.__class__.__name__, 0.001)

    def prepare(self) -> None:
        self._mqtt_client.connect(self._host, self._port, 60)

    def run_iteration(self) -> None:
        self._mqtt_client.loop_misc()
        self._mqtt_client.loop_read()
        if self._mqtt_client.want_write():
            self._mqtt_client.loop_write()

    def teardown(self) -> None:
        self._mqtt_client.disconnect()

    def on_connect(
        self,
        client: mqtt.Client,
        _: None,
        __: None,
        reason_code: mqtt.Properties,
        ___: None,
    ) -> None:
        if reason_code == 0:
            logging.info("Connected to MQTT broker")
            for metrics_manager in self._metrics_managers.values():
                client.subscribe(metrics_manager.mqtt_subscribe_prefix + "/#")
        else:
            raise Exception(f"Connected to MQTT broker with reason code '{reason_code}'")

    def on_disconnect(self, client: mqtt.Client, _: None, __: None, reason_code: mqtt.Properties, ___: None) -> None:
        if not self._running:
            # Don't do anything if program should exit
            return

        logging.warning(f"Disconnected from MQTT broker, reason code '{reason_code}', trying to reconnect...")

        # Try to reconnect a few times before giving up
        retry = 0
        while True:
            try:
                client.reconnect()
                break
            except (ConnectionRefusedError, TimeoutError) as e:
                if retry > 5:
                    raise e

                logging.warning(f"Failed to reconnect to MQTT broker, trying to reconnect in {2**retry} seconds...")
                time.sleep(2**retry)
                retry += 1

    @staticmethod
    def on_log(_: mqtt.Client, level: int, buf: str) -> None:
        if level == mqtt.MQTT_LOG_ERR:
            logging.error(f"MQTT ERROR: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            logging.warning(f"MQTT WARNING: {buf}")

    def on_message(self, _: mqtt.Client, __: None, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic

        try:
            payload = msg.payload.decode()
        except UnicodeDecodeError as decode_error:
            logging.error(f"Could not decode message bytes '{base64.encodebytes(msg.payload)}' payload: {decode_error}")
            return

        logging.debug(f"Received message {topic} {payload}")

        try:
            json_data = json.loads(payload)
        except ValueError as ex:
            # Ignore payloads that are not valid JSON
            logging.debug(f"Ignoring message that failed to parse as JSON: {topic} - {payload} - {ex}")
            return

        # Process the message
        split_topic = topic.split("/")
        self._metrics_managers[split_topic[0]].process_mqtt_message("/".join(split_topic[1:]), json_data)


def main() -> None:
    managers = []

    def exit_handler(signum: int = -1, _: types.FrameType | None = None) -> None:
        exit_code = 0
        if signum == signal.SIGINT:
            logging.info("SIGINT received, exiting...")
        elif signum == signal.SIGTERM:
            logging.info("SIGTERM received, exiting...")
        elif signum < 0:
            logging.error("Error occurred, exiting...")
            exit_code = 1

        for _manager in managers:
            _manager.stop()

        sys.exit(exit_code)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c",
        "--config",
        default="config.json",
        help="The configuration file for the converter.",
    )
    parser.add_argument("--hide-timestamps", action="store_true", help="Don't print timestamps to logging.")

    log_level_group = parser.add_mutually_exclusive_group()
    log_level_group.add_argument("--quiet", "-q", action="store_true", help="Only log warnings or higher.")
    log_level_group.add_argument("--debug", "-d", action="store_true", help="Show debug logs.")
    log_level_group.add_argument("--trace", "-t", action="store_true", help="Show debug and trace logs.")

    args = parser.parse_args()

    setup_logging(args.quiet, args.debug, args.trace, not args.hide_timestamps)

    # Load config
    config_path = pathlib.Path(args.config)
    if not config_path.is_file():
        logging.critical(f"Config file '{args.config}' does not exist.")
        exit_handler()

    with config_path.open("r") as fh:
        try:
            config = json.load(fh)
        except json.JSONDecodeError as e:
            logging.critical(f"Failed to decode config JSON: {e}")
            exit_handler()

    # Prepare threads
    exporter_config = config["prometheus_exporter"]

    metrics_managers = [
        TasmotaMetricsManager(
            exporter_config["filters"],
            exporter_config["cleanup"]["tasmota"]["interval"],
            exporter_config["cleanup"]["tasmota"]["threshold"],
        ),
        ShellyMetricsManager(
            exporter_config["filters"],
            exporter_config["cleanup"]["shelly"]["interval"],
            exporter_config["cleanup"]["shelly"]["threshold"],
        ),
        FaikoutMetricsManager(
            exporter_config["faikout"]["location"],
            exporter_config["filters"],
            exporter_config["cleanup"]["faikout"]["interval"],
            exporter_config["cleanup"]["faikout"]["threshold"],
        ),
        Zigbee2MQTTMetricsManager(
            exporter_config["filters"],
            exporter_config["cleanup"]["zigbee2mqtt"]["interval"],
            exporter_config["cleanup"]["zigbee2mqtt"]["threshold"],
        ),
        NoPrefixRawValuesManager(
            exporter_config["filters"],
            exporter_config["cleanup"]["noprefixraw"]["interval"],
            exporter_config["cleanup"]["noprefixraw"]["threshold"],
        ),
    ]
    managers.extend(metrics_managers)

    mqtt_config = config["mqtt"]
    managers.append(
        MQTTManager(
            metrics_managers,
            mqtt_config["host"],
            mqtt_config["port"],
            mqtt_config["user"],
            mqtt_config["password"],
            mqtt_config["tls"],
        )
    )

    # Set up signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    # Start manager threads
    for manager in managers:
        manager.start()

    prometheus_client.start_http_server(exporter_config["port"], exporter_config["bind_ip"])

    # Check for exit requests periodically
    while True:
        for manager in managers:
            if manager.exception:
                logging.critical(
                    f"Uncaught exception occurred in manager thread {manager.__class__.__name__}: "
                    f"{type(manager.exception).__name__} - {manager.exception}"
                )
                logging.debug(f"Stacktrace:\n{''.join(traceback.format_tb(manager.exception.__traceback__))}")
                exit_handler()

        time.sleep(0.1)


if __name__ == "__main__":
    main()
