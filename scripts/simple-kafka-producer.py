#!/usr/bin/env python3

import argparse
import json
import time
from datetime import datetime
from kafka import KafkaProducer

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--brokers",
                        dest="brokers",
                        nargs="+",
                        default=["localhost:9092"],
                        help="Kafka broker. Multiple can be defained separated by whitespace. Note that this option is only used for bootstrapping, so a single broker in cluster is enough.")

    parser.add_argument("--topic",
                        dest="topic",
                        default="test123",
                        help="topic to send messages to")

    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.brokers)

    i = 0
    while True:
        print("sending ", i)
        msg = {
            "syslog_message":   "message {}".format(i),
            "syslog_host":      "sycamore",
            "syslog_program":   "mytestprog",
            "syslog_ip":        "192.168.56.174",
            "@timestamp":       str(datetime.utcnow().astimezone().isoformat())
        }
        resp = producer.send(args.topic,
                             bytes(json.dumps(msg), encoding='utf-8'))
        print(resp)
        i += 1
        time.sleep(1)

    producer.close()
