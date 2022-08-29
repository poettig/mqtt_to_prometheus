#!/usr/bin/env python3

import argparse
import json
import logging
import math
import re
import signal
import os

import paho.mqtt.client as mqtt
import typing
from prometheus_client import Counter, Gauge, start_http_server


# https://stackoverflow.com/a/1176023/6371499
class CamelToSnakeConverter:
	def __init__(self):
		self.any_char_followed_by_uppercase_letter_pattern = re.compile(r"([^_])([A-Z][a-z]+)")
		self.lower_or_number_followed_by_uppercase_letter_pattern = re.compile(r"([a-z0-9])([A-Z])")

	def convert(self, string: str):
		result = self.any_char_followed_by_uppercase_letter_pattern.sub(r"\1_\2", string)
		result = self.lower_or_number_followed_by_uppercase_letter_pattern.sub(r"\1_\2", result)
		return result.lower()


camel_to_snake_converter = CamelToSnakeConverter()


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


class MetricsManager:
	def __init__(self, config):
		self.gauges: typing.Dict[str, Gauge] = dict()
		self.expected_label_keys = None
		self.message_counter = None
		self.drop_counter = None

		self.filters = {}
		for entry in config["filters"]:
			for metric_name in entry["metric_names"]:
				self.filters[metric_name] = {
					"rules": entry["rules"],
					"max_dropped_values": entry["max_dropped_values"],
					"already_dropped_values": 0
				}

		start_http_server(config["port"], config["bind_ip"])

	def extract_metrics(self, data):
		if isinstance(data, list):
			# Extract metrics for each list entry
			for entry in data:
				yield from self.extract_metrics(entry)
		elif isinstance(data, dict):
			# Iterate whole dict
			for key, value in data.items():
				if isinstance(value, list) or isinstance(value, dict):
					yield from self.extract_metrics(value)
				else:
					# Create gauge if value is numeric
					if isinstance(value, int) or isinstance(value, float):
						yield key, value

	def process_metrics(self, data: dict, labels):
		# Check if the label keys match the stored ones. If none are stored, initialize it and the counter
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
			prefixed_key = f"tasmota_{camel_to_snake_converter.convert(key)}"
			metric = Metric(prefixed_key, value, labels)

			if self.is_filtered(metric):
				self.drop_counter.labels(*(list(labels.values()))).inc()
				continue

			if prefixed_key not in self.gauges:
				self.create_gauge(metric)

			self.set_gauge_value(metric)

			if logging.root.level == logging.DEBUG:
				logging.debug(f"Set value of {metric.get_name()} to {metric.value}")

	def create_gauge(self, metric: Metric):
		self.gauges[metric.key] = Gauge(metric.key, "Value of the " + metric.key + " reading", metric.labels.keys())
		logging.debug(f"Created gauge {metric.get_name()}")

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

	def set_gauge_value(self, metric: Metric):
		self.gauges[metric.key].labels(*metric.labels.values()).set(metric.value)


class MQTTManager:
	relevant_message_types = [
		"STATE",
		"SENSOR",
	]

	def __init__(self, config: dict, metrics_manager: MetricsManager):
		self.metrics_manager = metrics_manager

		self.mqtt_client = mqtt.Client()
		self.mqtt_client.on_connect = self.on_connect
		self.mqtt_client.on_message = self.on_message
		self.mqtt_client.on_log = self.on_log
		if config["tls"]:
			self.mqtt_client.tls_set_context()
		self.mqtt_client.username_pw_set(config["user"], config["password"])
		self.mqtt_client.connect(config["host"], config["port"], 60)
		self.mqtt_client.loop_forever()

	def on_connect(self, client, _, __, rc):
		if rc == 0:
			logging.info("Connected to MQTT broker")
			client.subscribe("tele/#")
		else:
			logging.error("Connected to MQTT broker with return code " + str(rc))
			exithandler()

	def on_message(self, _, __, msg):
		topic = msg.topic
		payload = msg.payload.decode()

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

		# Convert every numeric value received into a gauge
		try:
			data = json.loads(payload)
			self.metrics_manager.process_metrics(data, labels)
		except ValueError as ex:
			logging.info(f"Ignored message {topic} {payload}: {ex}")
			pass

	def on_log(self, _, level, buf):
		if level == mqtt.MQTT_LOG_ERR:
			logging.error("MQTT ERROR: " + buf)
		elif level == mqtt.MQTT_LOG_WARNING:
			logging.warning("MQTT WARNING: " + buf)


def exithandler(signum=-1, stackframe=None):
	if signum == signal.SIGINT:
		logging.info("SIGINT received, exiting...")
	elif signum == signal.SIGTERM:
		logging.info("SIGTERM received, exiting...")
	elif signum < 0:
		logging.info("Error occured, exiting...")
		exit(1)

	exit(0)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output.")
	parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output.")
	parser.add_argument("--config", default="config.json", help="The configuration file for the converter.")
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
			conf = json.load(fh)
		except json.JSONDecodeError as e:
			logging.critical(f"Failed to decode config JSON: {e}")
			exithandler()

	# Setup signal handling
	signal.signal(signal.SIGINT, exithandler)
	signal.signal(signal.SIGTERM, exithandler)

	# Start prometheus server
	MQTTManager(conf["mqtt"], MetricsManager(conf["prometheus_exporter"]))
