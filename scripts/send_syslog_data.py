#!/usr/bin/env python
import argparse
import gzip
import socket
import re

pattern = re.compile("^(?P<pri><\d+>)?(?P<ts>\S+) (?P<host>\S+) (?P<program>\S+?)(?:\[(?P<pid>\d+)\])?:?(?P<msg> .+)$")

class SyslogBSD(object):

    """Docstring for SyslogBSD. """

    def __init__(self, msg):
        """TODO: to be defined.

        :TODO: TODO

        """
        matches = pattern.search(msg)

        if matches:
            self.Time = matches.group("ts")
            self.Pri = matches.group("pri") if matches.group("pri") else 1
            self.Host = matches.group("host")
            self.Program = matches.group("program")
            self.Pid = matches.group("pid") if matches.group("pid") else 666
            self.Message = matches.group("msg").rstrip()

    def Format(self):
        return "<{}>{} {} {}[{}]:{}".format(self.Pri, self.Time, self.Host, self.Program, self.Pid, self.Message)
        

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--logfile",
            dest="logfile",
            default=None,
            help="Input gzipped log file")

    parser.add_argument("--syslog-host",
            dest="syslog_host",
            default="localhost",
            help="Output syslog host.")

    parser.add_argument("--syslog-port",
            dest="syslog_port",
            type=int,
            default=514,
            help="Output syslog port.")

    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)

    with gzip.open(args.logfile, "rb") as reader:
        for l in reader:
            s = SyslogBSD(l.decode())
            sock.sendto(bytes(s.Format(), "utf-8"),
                    (args.syslog_host, args.syslog_port))

    sock.close()
