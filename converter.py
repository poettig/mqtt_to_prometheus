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
import typing

import paho.mqtt.client as mqtt
import prometheus_client
from prometheus_client import Counter, Gauge

prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)

json_data_type = dict[str, typing.Any] | list[dict[str, typing.Any] | str] | str
labels_dict_type = dict[str, str]


def setup_logging(quiet: bool, debug: bool, timestamps: bool) -> None:
    log_date_format = "%Y-%m-%d %H:%M:%S"
    log_format = "%(levelname)8s: %(message)s"

    if debug:
        log_level = logging.DEBUG
        log_format = "[%(name)s] " + log_format
    elif quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    if timestamps:
        log_format = "[%(asctime)s] " + log_format

    logging.basicConfig(level=log_level, format=log_format, datefmt=log_date_format)


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
        self._thread = threading.Thread(target=self._run)
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

            try:
                logging.debug(f"Run iteration loop of thread {self._thread.name}")
                self.run_iteration()
            except BaseException as e:
                self.exception = e
                self._running = False
                return

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
        # Does does nothing unless overwritten on purpose
        pass

    @abc.abstractmethod
    def run_iteration(self) -> None:
        raise NotImplementedError

    def teardown(self) -> None:  # noqa: B027
        # Does does nothing unless overwritten on purpose
        pass


class Metric(abc.ABC):
    counters: typing.ClassVar[dict[str, Counter]] = {}  # This is intended to be mutable, all metrics share their gauges
    counters_lock: threading.Lock = threading.Lock()

    gauges: typing.ClassVar[dict[str, Gauge]] = {}  # This is intended to be mutable, all metrics share their gauges
    gauges_lock: threading.Lock = threading.Lock()

    def __init__(self, name: str, labels: labels_dict_type, is_counter: bool = False, documentation: str = "") -> None:
        self.last_set: float = 0

        self._name = self._prefix + name
        self._labels = labels
        self._counter = None
        self._gauge = None

        store: dict[str, Counter | Gauge] = Metric.gauges
        lock = Metric.gauges_lock
        counter_or_gauge_initializer = Gauge
        if is_counter:
            store = Metric.counters
            lock = Metric.counters_lock
            counter_or_gauge_initializer = Counter

        # Lock complete section to not get a TOCTOU race condition
        with lock:
            counter_or_gauge = store.get(self._name)
            if counter_or_gauge is None:
                # Counter or Gauge does not exist, create it
                counter_or_gauge = counter_or_gauge_initializer(self._name, documentation, self._labels.keys())
                store[self._name] = counter_or_gauge

        if is_counter:
            self._counter = counter_or_gauge
        else:
            self._gauge = counter_or_gauge

    @staticmethod
    def create_metric_identifier(name: str, labels: labels_dict_type) -> str:
        # Using sorted(labels.items()) is fine here as values are always strings
        labelset_as_string = [key + "=" + value for key, value in sorted(labels.items())]
        return f"{name}{{{', '.join(labelset_as_string)}}}"

    @property
    @abc.abstractmethod
    def _prefix(self) -> str:
        raise NotImplementedError

    @property
    def _counter_or_gauge(self) -> Counter | Gauge:
        return self._counter if self._counter is not None else self._gauge

    @property
    def _metric(self) -> Counter | Gauge:
        return self._counter_or_gauge.labels(*self._labels.values())

    @property
    def _lock(self) -> threading.Lock:
        return Metric.counters_lock if self._counter is not None else Metric.gauges_lock

    @property
    def name(self) -> str:
        return self._name

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
        with self._lock:
            self._metric.inc(amount)

        self.last_set = time.time()
        logging.debug(f"Increased {self.full_name} by {amount} to {self.value}")

    def dec(self, amount: float = 1.0) -> None:
        if self._gauge is None:
            raise ValueError(f"Tried to decrease counter metric {self.full_name}")

        with self._lock:
            self._metric.dec(amount)

        self.last_set = time.time()
        logging.debug(f"Decreased {self.full_name} by {amount} to {self.value}")

    def set(self, value: float) -> None:
        if self._gauge is None:
            raise ValueError(f"Tried to set counter metric {self.full_name}")

        with self._lock:
            self._metric.set(value)

        self.last_set = time.time()
        logging.debug(f"Set {self.full_name} to {self.value}")

    def remove(self) -> bool:
        if self.last_set == 0:
            # Currently not set, removing makes no sense
            logging.debug(f"Tried to remove {self.full_name} but it is currently not set")
            return False

        with self._lock:
            self._counter_or_gauge.remove(*self._labels.values())

        self.last_set = 0
        logging.debug(f"Removed {self.full_name}")

        return True


class TasmotaMetric(Metric):
    @property
    def _prefix(self) -> str:
        return "tasmota_"


class ShellyMetric(Metric):
    @property
    def _prefix(self) -> str:
        return "shelly_"


class NoPrefixMetric(Metric):
    @property
    def _prefix(self) -> str:
        return ""


class MetricsManager(ThreadedManager, abc.ABC):
    def __init__(
        self,
        filters: dict,
        cleanup_interval: int,
        cleanup_threshold: int,
    ) -> None:
        self._message_counter = None
        self._drop_counter = None
        self._metrics: dict[str, Metric] = {}
        self._labels = None
        self._filters = {}
        self._cleanup_threshold = cleanup_threshold

        for entry in filters:
            for metric_name in entry["metric_names"]:
                self._filters[metric_name] = {
                    "rules": entry["rules"],
                    "max_dropped_values": entry["max_dropped_values"],
                    "already_dropped_values": 0,
                }

        super().__init__(self.__class__.__name__, cleanup_interval)

    @property
    @abc.abstractmethod
    def _metric_type(self) -> type[Metric]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def mqtt_subscribe_prefix(self) -> str:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def _extract_labels(topic: str) -> tuple[labels_dict_type, str] | None:
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
    def _extract_metrics(remaining_topic: str, json_data: json_data_type) -> list[tuple[str, float]]:
        """
        Extracts metrics data from the remaining topic after parsing labels and the JSON data of the message.

        :param remaining_topic: The remaining topic after labels were extracted
        :param json_data: The JSON data from the MQTT message

        :returns: A list of tuples where each tuple contains a metric name and its value
        """
        raise NotImplementedError


    @staticmethod
    def _create_metric_with_value(metric_prefix: str, metric_name: str, value: int | float) -> tuple[str, int | float]:
        full_metric_name = metric_name
        if metric_prefix:
            full_metric_name = f"{metric_prefix}_{metric_name}"

        return full_metric_name, value

    @staticmethod
    def _recursive_metrics_generator(json_data: json_data_type, prefix: str | None = None) -> typing.Generator[tuple[str, int | float]]:
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

                            yield MetricsManager._create_metric_with_value(prefix, metric_name, entry)

                        elif isinstance(entry, dict):
                            yield from MetricsManager._recursive_metrics_generator(entry, prefix=key)

                        # Lists in lists are ignored

                elif isinstance(value, list | dict):
                    yield from MetricsManager._recursive_metrics_generator(value, prefix=key)

                # Generate metric if the value is a number
                elif isinstance(value, int | float):
                    yield MetricsManager._create_metric_with_value(prefix, key, value)

    def get_metric(self, name: str, labels: labels_dict_type) -> Metric:
        # A specific metric is identified by its name and labels
        identifier = Metric.create_metric_identifier(name, labels)
        metric = self._metrics.get(identifier)
        if metric is None:
            metric = self._metric_type(name, labels)
            self._metrics[identifier] = metric
            logging.debug(f"Created new metric {metric.full_name}")

        return metric

    def should_be_filtered(self, metric: Metric, new_value: float) -> bool:
        filter_info = self._filters.get(metric.name)
        if filter_info is None:
            # No filter defined
            return False

        filter_messages = []
        for rule in filter_info["rules"]:
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
            if filter_info["already_dropped_values"] != 0:
                filter_info["already_dropped_values"] = 0
                logging.info(f"No filter hit for {metric}, reset drop counter.")

            return False

        filter_info["already_dropped_values"] += 1
        if filter_info["already_dropped_values"] >= filter_info["max_dropped_values"]:
            # Always accept when the maximum dropped values are reached
            filter_info["already_dropped_values"] = 0
            logging.warning(
                f"Accepted filtered value {metric} because maximum drops are reached. Drop reasons would have been:"
            )
            for filter_message in filter_messages:
                logging.warning(filter_message)

            return False

        logging.warning(
            f"[{filter_info['already_dropped_values']}/{filter_info['max_dropped_values']}]"
            f" Filtered new value for {metric.full_name}. Reasons:"
        )
        for filter_message in filter_messages:
            logging.warning(filter_message)

        return True

    def process_mqtt_message(self, topic: str, json_data: json_data_type) -> None:
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

        # Initialize counters if not done yet
        if self._labels is None:
            self._message_counter = self._metric_type(
                "processed_messages",
                labels,
                documentation="MQTT messages processed for this topic",
                is_counter=True,
            )
            self._drop_counter = self._metric_type(
                "dropped_values",
                labels,
                documentation="Number of metric values dropped because of a filter hit.",
                is_counter=True,
            )
        elif set(self._labels.keys()) != set(labels.keys()):
            raise ValueError(
                f"Label keys [{', '.join(labels.keys())}] "
                f"do not match the stored label keys [{', '.join(labels.keys())}] "
                f"from the first message."
            )

        self._message_counter.inc()

        for metric_name, value in metrics_data:
            metric = self.get_metric(CamelToSnakeConverter.convert(metric_name), labels)

            # Drop update if filtered, else set the gauge to the new value
            if self.should_be_filtered(metric, value):
                self._drop_counter.inc()
            else:
                metric.set(value)

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

    @staticmethod
    def _extract_labels(topic: str) -> tuple[labels_dict_type, str] | None:
        topic_elements = topic.split("/")
        if topic_elements[-1] not in TasmotaMetricsManager.message_types_to_parse:
            return None

        metric_labels_data = topic_elements[:-1]
        if len(metric_labels_data) % 2 != 0:
            raise ValueError(f"Labels extracted from topic are not an even number of elements: {metric_labels_data}")

        # Split labels data into keys and values
        metric_labels = {}
        metric_labels_data_iterator = iter(metric_labels_data)
        for key in metric_labels_data_iterator:
            metric_labels[key] = next(metric_labels_data_iterator)

        return metric_labels, ""

    @staticmethod
    def _extract_metrics(_: str, json_data: json_data_type) -> list[tuple[str, float]] | None:
        return list(MetricsManager._recursive_metrics_generator(json_data))

    @property
    def _metric_type(self) -> type[Metric]:
        return TasmotaMetric

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "tele"


class ShellyMetricsManager(MetricsManager):
    message_types_to_parse = ("status",)

    @staticmethod
    def _extract_labels(topic: str) -> tuple[labels_dict_type, str] | None:
        found_message_type = None
        for message_type in ShellyMetricsManager.message_types_to_parse:
            if f"/{message_type}" in topic:
                found_message_type = message_type
                break

        if found_message_type is None:
            return None

        topic_elements = topic.split("/")

        # Split labels data into keys and values
        metric_labels = {}
        remaining_topic = ""
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
    def _extract_metrics(remaining_topic: str, json_data: json_data_type) -> list[tuple[str, float]] | None:
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
    def _metric_type(self) -> type[Metric]:
        return ShellyMetric

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "shelly"


class NoPrefixRawValuesManager(MetricsManager):
    @property
    def _metric_type(self) -> type[Metric]:
        return NoPrefixMetric

    @property
    def mqtt_subscribe_prefix(self) -> str:
        return "noprefixraw"

    @staticmethod
    def _extract_labels(topic: str) -> tuple[labels_dict_type, str] | None:
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
    def _extract_metrics(remaining_topic: str, json_data: json_data_type) -> list[tuple[str, float]] | None:
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
        self._mqtt_client.on_connect = lambda *args, **kwargs: self.on_connect(*args, **kwargs)
        self._mqtt_client.on_disconnect = lambda *args, **kwargs: self.on_disconnect(*args, **kwargs)
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
        self, client: mqtt.Client, _: None, __: None, reason_code: mqtt.Properties | None, ___: None
    ) -> None:
        if reason_code == 0:
            logging.info("Connected to MQTT broker")
            for metrics_manager in self._metrics_managers.values():
                client.subscribe(metrics_manager.mqtt_subscribe_prefix + "/#")
        else:
            raise Exception(f"Connected to MQTT broker with reason code '{reason_code}'")

    @staticmethod
    def on_disconnect(client: mqtt.Client, _: None, __: None, reason_code: mqtt.Properties, ___: None) -> None:
        logging.warning(f"Disconnected from MQTT broker, reason code '{reason_code}', trying to reconnect")

        # Try to reconnect
        client.reconnect()

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

    def exit_handler(signum: int = -1, _: None = None) -> None:
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

    args = parser.parse_args()

    setup_logging(args.quiet, args.debug, not args.hide_timestamps)

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

    # Setup signal handling
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    # Start manager threads
    for manager in managers:
        manager.start()

    prometheus_client.start_http_server(exporter_config["port"], exporter_config["bind_ip"])

    # Check for exceptions and exit requests periodically
    while True:
        for manager in managers:
            if manager.exception:
                logging.critical(
                    f"Uncaught exception occurred in manager thread {manager.__class__.__name__} "
                    f"{type(manager.exception).__name__} - {manager.exception}"
                )
                logging.debug(f"Stacktrace:\n{''.join(traceback.format_tb(manager.exception.__traceback__))}")
                exit_handler()

        time.sleep(0.1)


if __name__ == "__main__":
    main()
