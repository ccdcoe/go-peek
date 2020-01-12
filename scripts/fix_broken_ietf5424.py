#!/usr/bin/env python

import gzip
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--in-logfile",
            dest="inLogfile",
            default=None,
            help="Input gzipped log file")

    parser.add_argument("--out-logfile",
            dest="outLogfile",
            default=None,
            help="Output gzipped log file")

    args = parser.parse_args()
    count = 0

    with gzip.open(args.inLogfile, 'rb') as reader:
        with gzip.open(args.outLogfile, 'wb') as writer:
            for l in reader:
                l = l.decode()
                cut = l.find(" ", l.find(" ") + 1)
                prefix = l[:cut]
                suffix = l[cut+1:]
                l =  prefix + " collector imkafka " + suffix.lstrip()
                writer.write(l.encode("utf-8"))

                count += 1
                if count%1000==0:
                    print(count)
