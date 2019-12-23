#!/usr/bin/env python3

import argparse
import json
import logging
import signal

import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, start_http_server

label_keys = None
messages_counter = None
gauges = dict()


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
        if key not in gauges:
            # Create new gauge
            gauges[key] = Gauge(key, "Value of the " + key + " reading", labels.keys())
            logging.debug("Created gauge " + key)
            
        # Set new metric value
        gauges[key].labels(*labels.values()).set(value)
        logging.debug("Set value of " + key + "{" + ",".join(labels.keys()) + "} to " + str(value))
    

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("tele/#")
    else:
        logging.error("Connected to MQTT broker with return code " + str(rc))
        exithandler()


def on_message(client, userdata, msg):
    global label_keys
    global messages_counter

    topic = msg.topic
    payload = msg.payload.decode()

    logging.debug("Received message " + payload)

    # Split and remove tele/
    topic_elems = topic.split("/")[1:]
    
    # Save name of metric
    name = topic_elems[-1]
    topic_elems = topic_elems[:-1]

    # Extract labels
    labels = {}
    if len(topic_elems) % 2 != 0:
        logging.error("Inner topic parts are not an even number of elements. Fix pls!")
        exithandler()
    else:
        it = iter(topic_elems)
        for key in it:
            labels[key] = next(it)

    # Check if the label keys match the stores ones. If none are stored, initialize it and the counter
    if label_keys is None:
        label_keys = list(labels.keys())
        messages_counter = Counter("processed_messages", "MQTT messages processed for this topic", label_keys + ["type"])
    elif label_keys != list(labels.keys()):
        logging.error("Label keys [" + ",".join(labels.keys()) + "] do not match the keys stored from the first message.")
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
        print("SIGINT received, exiting...")
    elif signum == signal.SIGTERM:
        print("SIGTERM received, exiting...")
    elif signum < 0:
        print("Error occured, exiting...")
        exit(1)

    exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug output.")
    parser.add_argument("-c", "--certpath", default="/etc/ssl/certs/", help="Path to the directory that stores CA certificates.")
    parser.add_argument("-mh", "--mqtthost", default="localhost", help="Address of MQTT broker to connect to.")
    parser.add_argument("-mp", "--mqttport", type=int, default=1883, help="Port of MQTT broker to connect to.")
    parser.add_argument("-pa", "--prometheusaddress", default="localhost", help="Address to bind to for prometheus metric exposition.")
    parser.add_argument("-pp", "--prometheusport", type=int, default=9337, help="Port to bind to for prometheus metric exposition.")
    parser.add_argument("-u", "--username", help="Username for MQTT broker authentication.")
    parser.add_argument("-p", "--password", help="Password for MQTT broker authentication.")
    args = parser.parse_args()

    # Check args
    if not 0 < args.prometheusport < 65536:
        logging.error("Invalid port number for prometheus.")
        exit(1)
    elif not 0 < args.mqttport < 65536:
        logging.error("Invalid port number for prometheus.")
        exit(1)

    # Setup signal handling
    signal.signal(signal.SIGINT, exithandler)
    signal.signal(signal.SIGTERM, exithandler)

    # Setup logging
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)

    # Start prometheus server
    start_http_server(args.prometheusport, args.prometheusaddress)

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_log = on_log
    mqtt_client.tls_set_context()
    mqtt_client.username_pw_set(args.username, args.password)
    mqtt_client.connect(args.mqtthost, args.mqttport, 60)
    mqtt_client.loop_forever()
