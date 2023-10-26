#!/usr/bin/env python3

import argparse
import base64
import json
import logging
import math
import re
import signal
import os
import threading
import time
import traceback

import paho.mqtt.client as mqtt
import typing
from prometheus_client import Counter, Gauge, start_http_server


# https://stackoverflow.com/a/1176023/6371499
class CamelToSnakeConverter:
	any_char_followed_by_uppercase_letter_pattern = re.compile(r"([^_])([A-Z][a-z]+)")
	lower_or_number_followed_by_uppercase_letter_pattern = re.compile(r"([a-z0-9])([A-Z])")

	@staticmethod
	def convert(string: str):
		result = re.sub("-", "_", string)
		result = CamelToSnakeConverter.any_char_followed_by_uppercase_letter_pattern.sub(r"\1_\2", result)
		result = CamelToSnakeConverter.lower_or_number_followed_by_uppercase_letter_pattern.sub(r"\1_\2", result)
		return result.lower()


class ThreadedManager:
	def __init__(self, thread_target: typing.Callable):
		self.running = False
		self.exception: typing.Optional[BaseException] = None
		self.thread_target = thread_target
		self.thread = threading.Thread(target=self._run)

	def _run(self):
		try:
			self.thread_target()
		except BaseException as e:
			self.exception = e

	def start_threaded_work(self):
		self.running = True
		self.thread.start()

	def stop_threaded_work(self):
		self.running = False
		self.thread.join()


class GaugeContainer(Gauge):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.last_set: typing.Dict[typing.Tuple, float] = dict()

	def labels(self, *args, **kwargs):
		self.last_set[args] = time.time()
		return super().labels(*args, **kwargs)


class Metric:
	def __init__(self, key, value, labels: dict):
		self.key = key
		self.value = value
		self.labels = labels

	def get_name(self):
		labels_as_strings = [key + '=' + value for key, value in self.labels.items()]
		return f"{self.key}{{{', '.join(labels_as_strings)}}}"

	def __str__(self):
		return f"{self.get_name()} {self.value}"


class MetricsManager(ThreadedManager):
	def __init__(self, bind_ip: str, port: int, filters: dict, cleanup_interval: int, cleanup_threshold: int):
		super().__init__(thread_target=self.cleanup_metrics_forever)

		self.gauges: typing.Dict[str, GaugeContainer] = dict()
		self.expected_label_keys = None
		self.message_counter = None
		self.drop_counter = None
		self.cleanup_interval = cleanup_interval
		self.cleanup_threshold = cleanup_threshold
		self.filters = {}

		for entry in filters:
			for metric_name in entry["metric_names"]:
				self.filters[metric_name] = {
					"rules": entry["rules"],
					"max_dropped_values": entry["max_dropped_values"],
					"already_dropped_values": 0
				}

		start_http_server(port, bind_ip)

	def extract_metrics(self, data, prefix=None):
		if isinstance(data, list):
			# Extract metrics for each list entry
			for entry in data:
				yield from self.extract_metrics(entry)
		elif isinstance(data, dict):
			# Iterate whole dict
			for key, value in data.items():
				if isinstance(value, list) or isinstance(value, dict):
					yield from self.extract_metrics(value, prefix=key)
				else:
					# Create gauge if value is numeric
					if isinstance(value, int) or isinstance(value, float):
						full_key = key
						if prefix:
							full_key = f"{prefix}_{key}"

						yield full_key, value

	def create_gauge(self, metric: Metric):
		self.gauges[metric.key] = GaugeContainer(metric.key, "Value of the " + metric.key + " reading", metric.labels.keys())
		logging.debug(f"Created gauge {metric.get_name()}")

	def set_gauge_value(self, metric: Metric):
		self.gauges[metric.key].labels(*metric.labels.values()).set(metric.value)

	def is_filtered(self, metric: Metric):
		filter_info = self.filters.get(metric.key)
		if filter_info is None:
			# No filter defined
			return False

		gauge = self.gauges.get(metric.key)
		if gauge is None:
			# The gauge does not exist yet, no value to compare against
			return False

		for rule in filter_info["rules"]:
			rule_type, rule_value = rule.split(":")
			rule_value = float(rule_value)

			x = gauge.labels(*metric.labels.values())
			previous_value = x._value.get()

			if rule_type == "diff":
				if abs(metric.value - previous_value) > rule_value:
					filter_message = f"{metric.value} differs from previous value {previous_value} by more than {rule_value}."
					break

			elif rule_type == "above":
				if metric.value > rule_value:
					filter_message = f"{metric.value} is above {rule_value}."
					break

			elif rule_type == "below":
				if metric.value < rule_value:
					filter_message = f"{metric.value} is below {rule_value}."
					break

			elif rule_type == "value":
				if math.isclose(metric.value, rule_value):
					filter_message = f"{metric.value} is a forbidden value."
					break

			else:
				raise ValueError(f"Invalid rule type '{rule_type}'.")

		else:
			# No filter was hit
			if filter_info["already_dropped_values"] != 0:
				filter_info["already_dropped_values"] = 0
				logging.info(f"No filter hit for {metric}, reset drop counter.")
			return False

		filter_info["already_dropped_values"] += 1

		if filter_info["already_dropped_values"] >= filter_info["max_dropped_values"]:
			# Always accept when the maximum dropped values are reached
			filter_info["already_dropped_values"] = 0
			logging.warning(
				f"Accepted filtered value {metric} because maximum drops are reached."
				f" Drop reason would have been '{filter_message}'."
			)
			return False

		logging.warning(
			f"[{filter_info['already_dropped_values']}/{filter_info['max_dropped_values']}] Filtered {metric}:"
			f" {filter_message}"
		)
		return True

	def process_metrics(self, data: dict, labels):
		# Check if the label keys match the stored ones.
		# If none are stored, initialize it and the counter
		if self.expected_label_keys is None:
			self.expected_label_keys = list(labels.keys())
			self.message_counter = Counter(
				"tasmota_processed_messages",
				"MQTT messages processed for this topic",
				self.expected_label_keys
			)
			self.drop_counter = Counter(
				"tasmota_dropped_values",
				"Number of metric values dropped because of a filter hit.",
				self.expected_label_keys
			)
		elif self.expected_label_keys != list(labels.keys()):
			logging.error(
				"Label keys [" + ",".join(labels.keys()) + "] do not match the keys stored from the first message."
			)
			exithandler()

		self.message_counter.labels(*(list(labels.values()))).inc()

		for key, value in self.extract_metrics(data):
			prefixed_key = f"tasmota_{CamelToSnakeConverter.convert(key)}"
			metric = Metric(prefixed_key, value, labels)

			if self.is_filtered(metric):
				self.drop_counter.labels(*(list(labels.values()))).inc()
				continue

			if prefixed_key not in self.gauges:
				self.create_gauge(metric)

			self.set_gauge_value(metric)

			if logging.root.level == logging.DEBUG:
				logging.debug(f"Set value of {metric.get_name()} to {metric.value}")

	def cleanup_metrics_forever(self):
		interval_start = 0
		while self.running:
			# This is done in order to exit fast if requested by the main thread
			if time.time() - interval_start < self.cleanup_interval:
				time.sleep(.1)
				continue

			logging.debug("Running metrics cleanup...")
			for key, gauge in self.gauges.items():

				# Only identify entries to delete.
				# Otherwise, deletion of the last_set entry will cause a "dict size change during iteration" error.
				to_delete = []
				for labelvalues, last_set in gauge.last_set.items():
					if time.time() - last_set > self.cleanup_threshold:
						to_delete.append(labelvalues)

				for labelvalues in to_delete:
					logging.info(f"Removing gauge {key}[{', '. join(labelvalues)}] as it was inactive for {self.cleanup_threshold}s.")
					gauge.remove(*labelvalues)
					del gauge.last_set[labelvalues]

			# Reset interval start time to start the next interval
			interval_start = time.time()


class MQTTManager(ThreadedManager):
	relevant_message_types = [
		"STATE",
		"SENSOR",
	]

	def __init__(self, metrics_manager: MetricsManager, host: str, port: int, user: str, password: str, tls: bool):
		super().__init__(thread_target=self.connect_and_loop)

		self.metrics_manager = metrics_manager

		self.mqtt_client = mqtt.Client()
		self.mqtt_client.on_connect = self.on_connect
		self.mqtt_client.on_message = self.on_message
		self.mqtt_client.on_log = self.on_log
		self.mqtt_client.username_pw_set(user, password)
		if tls:
			self.mqtt_client.tls_set_context()

		self.host = host
		self.port = port

	def connect_and_loop(self):
		self.mqtt_client.connect(self.host, self.port, 60)
		self.mqtt_client.loop_start()

		while self.running:
			time.sleep(.1)

		self.mqtt_client.loop_stop()

	@staticmethod
	def on_connect(client, _, __, rc):
		if rc == 0:
			logging.info("Connected to MQTT broker")
			client.subscribe("tele/#")
		else:
			logging.error("Connected to MQTT broker with return code " + str(rc))
			exithandler()

	@staticmethod
	def on_log(_, level, buf):
		if level == mqtt.MQTT_LOG_ERR:
			logging.error("MQTT ERROR: " + buf)
		elif level == mqtt.MQTT_LOG_WARNING:
			logging.warning("MQTT WARNING: " + buf)

	def on_message(self, _, __, msg):
		topic = msg.topic

		try:
			payload = msg.payload.decode()
		except UnicodeDecodeError as decode_error:
			logging.error(f"Could not decode message bytes '{base64.encodebytes(msg.payload)}' payload: {decode_error}")
			return

		logging.debug(f"Received message {topic} {payload}")

		# Split and remove tele/
		topic_elems = topic.split("/")[1:]

		# Save name of metric
		name = topic_elems[-1]
		topic_elems = topic_elems[:-1]

		# Extract labels
		labels = {}
		if len(topic_elems) % 2 != 0:
			logging.error("Inner topic parts are not an even number of elements. Fix pls!")
			logging.error(f"{topic_elems}")
			exithandler()
		else:
			it = iter(topic_elems)
			for key in it:
				labels[key] = next(it)

		# Check if the last part of the topic is one of the relevant message types
		if name not in MQTTManager.relevant_message_types:
			logging.debug(f"Ignored message: {topic} {payload}")
			return

		# Read payload as JSON
		# If the payload isn't valid JSON, ignore it
		try:
			data = json.loads(payload)
		except ValueError as ex:
			logging.info(f"Ignored message {topic} {payload}: {ex}")
			return

		# Process the message
		self.metrics_manager.process_metrics(data, labels)


managers: typing.List[ThreadedManager] = []


def exithandler(signum=-1, _=None):
	exit_code = 0
	if signum == signal.SIGINT:
		logging.info("SIGINT received, exiting...")
	elif signum == signal.SIGTERM:
		logging.info("SIGTERM received, exiting...")
	elif signum < 0:
		logging.info("Error occured, exiting...")
		exit_code = 1

	for manager in managers:
		manager.stop_threaded_work()

	exit(exit_code)


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output.")
	parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output.")
	parser.add_argument("-c", "--config", default="config.json", help="The configuration file for the converter.")
	args = parser.parse_args()

	# Setup logging
	if args.debug:
		log_level = logging.DEBUG
	elif args.verbose:
		log_level = logging.INFO
	else:
		log_level = logging.WARN

	logging.basicConfig(level=log_level, format="[%(asctime)s] %(levelname)8s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

	# Load config
	if not os.path.isfile(args.config):
		logging.critical(f"Config file '{args.config}' does not exist.")
		exithandler()

	with open(args.config, "r") as fh:
		try:
			config = json.load(fh)
		except json.JSONDecodeError as e:
			logging.critical(f"Failed to decode config JSON: {e}")
			exithandler()

	# Prepare threads
	exporter_config = config["prometheus_exporter"]
	metrics_manager = MetricsManager(
		exporter_config["bind_ip"],
		exporter_config["port"],
		exporter_config["filters"],
		exporter_config["cleanup"]["interval"],
		exporter_config["cleanup"]["threshold"]
	)
	managers.append(metrics_manager)

	mqtt_config = config["mqtt"]
	managers.append(MQTTManager(
		metrics_manager,
		mqtt_config["host"],
		mqtt_config["port"],
		mqtt_config["user"],
		mqtt_config["password"],
		mqtt_config["tls"]
	))

	# Setup signal handling
	signal.signal(signal.SIGINT, exithandler)
	signal.signal(signal.SIGTERM, exithandler)

	# Start manager threads
	for manager in managers:
		manager.start_threaded_work()

	# Check for exceptions periodically
	while True:
		for manager in managers:
			if manager.exception:
				logging.critical(
					f"Uncaught exception occured in thread {manager.thread.name}: "
					f"{type(manager.exception).__name__} - {manager.exception}"
				)
				logging.debug(f"Stacktrace:\n{''.join(traceback.format_tb(manager.exception.__traceback__))}")
				exithandler()

		time.sleep(.1)


if __name__ == "__main__":
	main()
