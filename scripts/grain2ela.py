#!/usr/bin/env python3

# Slightly modified version of
# https://gist.github.com/markuskont/499ee5113ecaf63e7f98c8a4b2343f1f
#
# apt-get install python-pip
# pip install elasticsearch

import sys, json, argparse, time, hashlib
import salt.client
import salt.config
from elasticsearch import Elasticsearch

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-es','--elasticsearch', nargs='+', required=False, default=['localhost'])
    parser.add_argument('-i', '--index', default='inventory')
    parser.add_argument('-t', '--type', default='grains')
    parser.add_argument('-sc', '--saltconfig', default='/etc/salt/master')
    parser.add_argument('-p', '--pipeline', default=None)
    return parser.parse_args()

ARGS = parse_arguments()
HOSTS=ARGS.elasticsearch
INDEX=ARGS.index
TYPE=ARGS.type
PIPELINE=ARGS.pipeline

SALT_TIMEOUT=60

def saltQuery():
    return salt.client.LocalClient(c_path=ARGS.saltconfig).cmd(
        tgt='*',
        fun='grains.items',
        tgt_type='glob',
        timeout=SALT_TIMEOUT
)

if __name__ == '__main__':
    es = Elasticsearch(hosts=HOSTS, timeout=60)

    timestamp = int(round(time.time()))

    grains = saltQuery()
    bulk = []
    timeseries = "%s-%s" % (INDEX, time.strftime('%Y', time.localtime(timestamp)))
    latest = "%s-latest" % (INDEX)

    for host, data in grains.items():
        if not data:
            print("No data for {}. Skipping...".format(host))
            continue
        try:
            pv = '.'.join([str(i) for i in data['pythonversion']])
            data['pythonversion'] = pv
        except:
            print("Can't stringify python version. Skipping {}...".format(host))
            continue
        meta = {
            "index": {
                "_index": timeseries,
                "_type": TYPE
            }
        }
        data['@timestamp'] = timestamp * 1000

        # create immutable log of historical data (dynamic ID)
        bulk.append(json.dumps(meta))
        bulk.append(json.dumps(data))

        # create mutable report of current (static ID)
        # fqdn is already a unique id for saltstack, thus good enough for ES
        # also use different index
        meta['index']['_id'] = data['fqdn']
        meta['index']['_index'] = latest

        bulk.append(json.dumps(meta))
        bulk.append(json.dumps(data))

    ## Debug
    # for item in bulk:
    #     print item
    
    # push bulk to ES
    print(json.dumps(es.bulk(body=bulk), sort_keys=True, indent=2))
