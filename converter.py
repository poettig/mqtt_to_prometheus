#!/usr/bin/env python3

import argparse
import json
import logging
import re
import signal
import os

import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, start_http_server

relevant_message_types = [
	"STATE",
	"SENSOR",
]

label_keys = None
messages_counter = None
gauges = dict()


# https://stackoverflow.com/a/1176023/6371499
class CamelToSnakeConverter:
	def __init__(self):
		self.any_char_followed_by_uppercase_letter_pattern = re.compile(r"(.)([A-Z][a-z]+)")
		self.lower_or_number_followed_by_uppercase_letter_pattern = re.compile(r"([a-z0-9])([A-Z])")

	def convert(self, string: str):
		result = self.any_char_followed_by_uppercase_letter_pattern.sub(r"\1_\2", string)
		result = self.lower_or_number_followed_by_uppercase_letter_pattern.sub(r"\1_\2", result)
		return result.lower()


camel_to_snake_converter = CamelToSnakeConverter()


def extract_metrics(data):
	if isinstance(data, list):
		# Extract metrics for each list entry
		for entry in data:
			yield from extract_metrics(entry)
	elif isinstance(data, dict):
		# Iterate whole dict
		for key, value in data.items():
			if isinstance(value, list) or isinstance(value, dict):
				yield from extract_metrics(value)
			else:
				# Create gauge if value is numeric
				if isinstance(value, int) or isinstance(value, float):
					yield key, value


def parse_metrics(data, labels):
	for key, value in extract_metrics(data):
		prefixed_key = f"tasmota_{camel_to_snake_converter.convert(key)}"

		if prefixed_key not in gauges:
			# Create new gauge
			gauges[prefixed_key] = Gauge(prefixed_key, "Value of the " + prefixed_key + " reading", labels.keys())
			logging.debug("Created gauge " + prefixed_key)

		# Set new metric value
		gauges[prefixed_key].labels(*labels.values()).set(value)

		if logging.root.level == logging.DEBUG:
			labels_as_strings = [key + '=' + value for key, value in labels.items()]
			logging.debug(f"Set value of {prefixed_key}{{{', '.join(labels_as_strings)}}} to {value}")


def on_connect(client, userdata, flags, rc):
	if rc == 0:
		logging.info("Connected to MQTT broker")
		client.subscribe("tele/#")
	else:
		logging.error("Connected to MQTT broker with return code " + str(rc))
		exithandler()


def on_message(client, userdata, msg):
	global label_keys
	global messages_counter

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
	if name not in relevant_message_types:
		logging.debug(f"Ignored message: {topic} {payload}")
		return

	# Check if the label keys match the stores ones. If none are stored, initialize it and the counter
	if label_keys is None:
		label_keys = list(labels.keys())
		messages_counter = Counter(
			"processed_messages",
			"MQTT messages processed for this topic",
			label_keys + ["type"]
		)
	elif label_keys != list(labels.keys()):
		logging.error(
			"Label keys [" + ",".join(labels.keys()) + "] do not match the keys stored from the first message."
			)
		exithandler()

	labels_for_counter = list(labels.values()) + [name]
	messages_counter.labels(*labels_for_counter).inc()

	# Convert every numeric value received into a gauge
	try:
		data = json.loads(payload)
		parse_metrics(data, labels)
	except ValueError:
		logging.info("Ignored message: " + topic + " " + payload)
		pass


def on_log(client, userdata, level, buf):
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

	# Setup signal handling
	signal.signal(signal.SIGINT, exithandler)
	signal.signal(signal.SIGTERM, exithandler)

	# Setup logging
	if args.debug:
		logging.basicConfig(level=logging.DEBUG)
	elif args.verbose:
		logging.basicConfig(level=logging.INFO)
	else:
		logging.basicConfig(level=logging.WARN)

	# Start prometheus server
	prometheus_exporter_config = config["prometheus_exporter"]
	start_http_server(prometheus_exporter_config["port"], prometheus_exporter_config["bind_ip"])

	mqtt_config = config["mqtt"]
	mqtt_client = mqtt.Client()
	mqtt_client.on_connect = on_connect
	mqtt_client.on_message = on_message
	mqtt_client.on_log = on_log
	if mqtt_config["tls"]:
		mqtt_client.tls_set_context()
	mqtt_client.username_pw_set(mqtt_config["user"], mqtt_config["password"])
	mqtt_client.connect(mqtt_config["host"], mqtt_config["port"], 60)
	mqtt_client.loop_forever()
