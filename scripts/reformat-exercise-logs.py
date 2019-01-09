#!/usr/bin/env python

"""
This is a simple script that reads xs19 test run data and outputs reformats json keys for go-peek
Some keys were changed after exercise, snoopy payload needed to be reparsed, etc
Uploaded for reference
Only depends on modern python - written and tested with python 3.7.2 on Arch Linux
Only stdlib and minimal object-orientation! Meant to be compact, not performant!!
Some exception handling, but don't expect anything special
Reads from and writes to gzipped log files with 1:1 filename mapping
So, not suitable for larger datasets.
"""

import gzip
import json
import re

from os import listdir
from os.path import isfile, join

class KeyCollisionException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class ArgumentMissingException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

def isSnoopy(program):
    return True if "snoopy" in program.lower() else False

def isSysmon(program):
    return True if "sysmon" in program.lower() else False

def isSuriAlert(evtype):
    return True if "alert" in evtype.lower() else False

def isSuriStat(evtype):
    return True if "stats" in evtype.lower() else False

def isSuriFlow(evtype):
    return True if "flow" in evtype.lower() else False

def onlyFiles(mypath):
    return [f for f in listdir(mypath) if isfile(join(mypath, f))]

def syslogReMap():
    return {
            "@timestamp":       "@timestamp",
            "syslog_host":      "host",
            "syslog_program":   "program",
            "syslog_severity":  "severity",
            "syslog_facility":  "facility",
            "syslog_ip:":       "ip",
            }

def syslogMessageFix(orig):
    new = {
            "@timestamp":       orig["@timestamp"],
            "syslog_host":      orig["host"],
            "syslog_program":   orig["program"],
            "syslog_severity":  orig["severity"],
            "syslog_facility":  orig["facility"],
            "syslog_ip:":       orig["ip"],
            }
    if "message" in orig: 
        new["syslog_message"] = orig["message"]
    return new

def dropKeys(keys, data):
    return (lambda x: data.pop(x, None), keys)

def simpleSnoopy(mgrp):
    return {
            "uid":      mgrp[0],
            "sid":      mgrp[1],
            "tty":      mgrp[2],
            "cwd":      mgrp[3],
            "filename": mgrp[4],
            }

def expandedSnoopy(mgrp):
    return {
            "login":        mgrp[0],
            "ssh":          parseSnoopySSH(mgrp[1]),
            "username":     mgrp[2],
            "uid":          mgrp[3],
            "group":        mgrp[4],
            "gid":          mgrp[5],
            "sid":          mgrp[6],
            "tty":          mgrp[7],
            "cwd":          mgrp[8],
            "filename":     mgrp[9],
            "cmd":          mgrp[10],
            }

def parseSnoopySSH(msg):
    if "undefined" in msg:
        return {}
    msg = msg.lstrip("(").rstrip(")")
    fields = msg.split()
    return {
            "src_ip":   fields[0],
            "src_port": fields[1],
            "dst_ip":   fields[2],
            "dst_port": fields[3],
            }

def outFiles(name, filelist):
    return [ gzip.open(join(root, name), 'wb') for root in filelist ]

def JoinMaps(initial, second, exceptions=[]):
    for k, v in second.items():
        if k in initial and k not in exceptions: 
            raise KeyCollisionException("{} exists in initial message".format(k))
        initial[k] = v 
    return initial

def streamGzipFileAndWriteModfifiedOutput(infile, modifyFunc, **kwargs):
    if not kwargs:
        raise ArgumentMissingException("kwargs files missing for {}".format(infile))

    if "outfiles" not in kwargs:
        raise ArgumentMissingException("output files missing for {}".format(infile))
    outfiles = kwargs["outfiles"]
    #print(outfiles)

    ok = 0
    bad = 0
    with gzip.open(infile, "rb") as inpt:
        for line in inpt:
            try:
                modifyFunc(line, **kwargs)
                ok += 1
            except Exception as e:
                bad += 1
        inpt.close()
        print("done reading {}, ok: {}, bad {} lines".format(infile, str(ok), str(bad)))

    for fh in outfiles:
        fh.close()

def readSyslogLineAndWriteOutput(line, **kwargs):
    if "snoopypatterns" not in kwargs:
        raise ArgumentMissingException("snoopy patterns missing")
    snoopypatterns = kwargs["snoopypatterns"]

    if "outfiles" not in kwargs:
        raise ArgumentMissingException("output files missing")
    outfiles = kwargs["outfiles"]

    try:
        line = line.decode("latin-1")
        orig = json.loads(line)
        new = syslogMessageFix(orig)
        out = outfiles[1] if isSnoopy(orig["program"]) else outfiles[0]

        if isSnoopy(orig["program"]):
            match = [m.match(orig["message"]) for m in snoopypatterns]
            groups = [m.groups() if m else None for m in match]
            if match[0] or match[1]:
                parsed = simpleSnoopy(groups[0]) if match[0] else expandedSnoopy(groups[1])
                try:
                    new = JoinMaps(new, parsed)
                except KeyCollisionException as e:
                    print("Key collision. Syslog: {}. Snoopy: {}. Key: {}",
                            json.dumps(new),
                            json.dumps(parsed),
                            e)
            else:
                print("broken regex:", orig["message"])
        out.write(json.dumps(new).encode(encoding="utf-8"))
    except UnicodeDecodeError as e:
        print(e, line)
    except Exception as e:
        print(e, line)
        raise e

def readEventLogLineAndWriteOutput(line, **kwargs):
    if "outfiles" not in kwargs:
        raise ArgumentMissingException("output files missing")
    outfiles = kwargs["outfiles"]

    try:
        line = line.decode("latin-1")
        line = line.replace('\\"', '"', -1)
        line = line.replace('\\\\', '\\', -1)
        orig = json.loads(line)
        out = outfiles[1] if isSysmon(orig["program"]) else outfiles[0]

        new = {key: value for (key, value) in orig.items() if key not in list(syslogReMap().values())}
        new = JoinMaps(new, syslogMessageFix(orig), ["@timestamp"])
        out.write(json.dumps(new).encode(encoding="utf-8"))
    except UnicodeDecodeError as e:
        print(e, line)
    except Exception as e:
        print(e, line)
        raise e

def readSuricataLogLineAndWriteOutput(line, **kwargs):
    if "outfiles" not in kwargs:
        raise ArgumentMissingException("output files missing")
    outfiles = kwargs["outfiles"]

    try:
        line = line.decode("latin-1")

        line = line.replace('\\"', '"', -1)
        line = line.replace('\\\\', '\\', -1)
        orig = json.loads(line)

        if isSuriAlert(orig["event_type"]):
            out = outfiles[0]
        elif isSuriStat(orig["event_type"]):
            out = outfiles[2]
        elif isSuriFlow(orig["event_type"]):
            out = outfiles[3]
        else:
            out = outfiles[1]

        new = {key: value for (key, value) in orig.items() if key not in list(syslogReMap().values())}
        new = JoinMaps(new, syslogMessageFix(orig), ["@timestamp"])
        out.write(json.dumps(new).encode(encoding="utf-8"))

    except UnicodeDecodeError as e:
        print(e, line)
    except Exception as e:
        print(e, line)
        raise e

def LinuxAndSnoopy(mypath="./xs19-tr-linux",
        linux="./reformat/xs19-tr-linux",
        snoopy="./reformat/xs19-tr-snoopy"):

    snoopypatterns = [
            "^\s?\[uid:(\d+) sid:(\d+) tty:(\S+) cwd:(\S+) filename:(\S+)\]: (.+)$",
            "^\s?\[login:(\S+) ssh:(\(.+?\)) username:(\S+) uid:(\d+) group:(\S+) gid:(\d+) sid:(\d+) tty:(\S+) cwd:(\S+) filename:(\S+)\]: (.+)$",
            ]
    snoopypatterns = [re.compile(pattern) for pattern in snoopypatterns]

    done = 0
    for f in onlyFiles(mypath):
        streamGzipFileAndWriteModfifiedOutput(join(mypath, f),
                readSyslogLineAndWriteOutput,
                outfiles=outFiles(f, [linux, snoopy]),
                snoopypatterns=snoopypatterns)
        done += 1
    return done

def WindowsAndSysmon(mypath="./xs19-tr-windows",
        windows="./reformat/xs19-tr-windows",
        sysmon="./reformat/xs19-tr-sysmon"):

    done = 0
    for f in onlyFiles(mypath):
        streamGzipFileAndWriteModfifiedOutput(join(mypath, f),
                readEventLogLineAndWriteOutput,
                outfiles=outFiles(f, [windows, sysmon]))
        done += 1
    return done

def SuricataAlertAndOthers(mypath="./xs19-tr-suricata",
        alert="./reformat/xs19-tr-alert",
        protocols="./reformat/xs19-tr-protocols",
        stats="./reformat/xs19-tr-stats",
        flow="./reformat/xs19-tr-flow"):

    done = 0
    for f in onlyFiles(mypath):
        streamGzipFileAndWriteModfifiedOutput(join(mypath, f),
                readSuricataLogLineAndWriteOutput,
                outfiles=outFiles(f, [alert, protocols, stats, flow]))
        done += 1
    return done

if __name__ == "__main__":
    tasks = [SuricataAlertAndOthers, WindowsAndSysmon, LinuxAndSnoopy]
    for task in tasks:
        try:
            files = task()
            print("Reparsed {} files".format(str(files)))
        except Exception as e:
            print(e)
