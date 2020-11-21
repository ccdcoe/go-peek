#!/usr/bin/env python3

import argparse
import glob
import gzip
import socket

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--glob", dest="glob", default=None)
    parser.add_argument("--socket", dest="sock", default=None)
    args = parser.parse_args()

    files = glob.glob(args.glob)
    files = sorted(files)
    print("got {} files ".format(len(files)))

    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(args.sock)

    for file in files:
        print(file)
        with gzip.open(file, "r") as handle:
            for line in handle:
                client.send(line)
