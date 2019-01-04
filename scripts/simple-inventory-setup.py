#!/usr/bin/env python

import json
from elasticsearch import Elasticsearch

stuff = {}
with open("test/data/grains.json", "r") as f:
    for line in f:
        data = json.loads(line)
        for k, v in data.items():
            stuff[k] = v

es = Elasticsearch()
bulk = []

for k, v in stuff.items():
    try:
        pv = '.'.join([str(i) for i in v['pythonversion']])
        v['pythonversion'] = pv
    except:
        print("Can't stringify python version. Skipping {}...".format(k))
        continue

    meta = {
            "index": {
                "_id": v["fqdn"],
                "_type": "doc",
                "_index": "inventory-latest",
                }
            }
    bulk.append(json.dumps(meta))
    bulk.append(json.dumps(v))

print(json.dumps(es.bulk(body=bulk), sort_keys=True, indent=2))
