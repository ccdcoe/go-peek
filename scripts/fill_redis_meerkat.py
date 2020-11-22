#!/usr/bin/env python

import argparse
import redis
import json

import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-host",
                        dest="redis_host",
                        default="localhost")
    parser.add_argument("--redis-port",
                        dest="redis_port",
                        default=6379)
    parser.add_argument("--redis-db",
                        dest="redis_db",
                        default=0)
    parser.add_argument("--meerkat-csv",
                        dest="meerkat_csv",
                        default=None)
    args = parser.parse_args()

    df = pd.read_csv(args.meerkat_csv)

    r = redis.Redis(host=args.redis_host,
                    port=args.redis_port,
                    db=args.redis_db)

    for item in df.to_dict(orient="records"):
        print("setting {}".format(item.get("sid")))
        r.set(item.get("sid"), json.dumps(item))
