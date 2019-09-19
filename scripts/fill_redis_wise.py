#!/usr/bin/env python

import argparse
import redis
import json
import sys
import gzip
import hashlib

def read_gzip(arg: str):
    with gzip.open(arg, 'rb') as grains:
        for g in grains: yield g

def read_regular(arg: str):
    with open(arg, 'rb') as grains:
        for g in grains: yield g

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--asset-csv-file",
            dest="assets",
            default=None,
            help="CSV file containing name mappings and addresses.")

    parser.add_argument("--assets-gzipped",
            dest="assets_gzipped",
            action="store_true",
            default=False,
            help="Read assets from gzip file instead.")

    parser.add_argument("--field-prefix",
            dest="field_prefix",
            default="peek",
            help="Prefix, or group for fields. Moloch field names would be <prefix>.<field>")

    parser.add_argument("--redis-host",
            dest="redis_host",
            default="localhost",
            help="Output redis host.")

    parser.add_argument("--redis-port",
            dest="redis_port",
            type=int,
            default=6379,
            help="Output redis port.")

    args = parser.parse_args()

    if args.assets is None:
        sys.exit(1)

    fn = read_gzip if args.assets_gzipped else read_regular
    keys = []
    assets = []
    for i, item in enumerate(fn(args.assets)):
        item = item.decode().strip().split(",")
        if i is 0: 
            keys = item
        else:
            assets.append(dict(zip(keys, item)))

    wise_redis_data = {}
    for asset in assets:
        if "ip" not in asset: continue

        fields = []
        for k, v in asset.items():
            if k == "ip": continue
            k = ".".join([args.field_prefix, k])
            fields.append("=".join([k, v]))
        tagger = "{};{}".format(asset["ip"], ";".join(fields))

        print(tagger)
        wise_redis_data[asset["ip"]] = tagger

    r = redis.Redis(host=args.redis_host, port=args.redis_port, db=0)
    for k, v in wise_redis_data.items():
        r.set(k, v)
