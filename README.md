# go-peek
> Peek is a simple streaming pre-processor and enrichment tool for structured logs. 

Peek was written to be central data normalization engine for [Frankenstack](https://github.com/ccdcoe/frankenstack), Crossed Swords Cyber Defense Exercise Yellow team feedback system. Originally developed during 2018 [Frankencoding](https://github.com/ccdcoe/Frankencoding) hackathon, as a lightweight alternative to general purpose log processing tools (e.g., LogStash, Rsyslog) and custom (nasty) Python scripts used in prior exercise iterations. Each message is enriched using inventory information from targets to simplify event correlation, no normalize addressing (i.e. IPv6 short format vs long), etc.

Current version is designed to consume events from one Kafka cluster and produce transformed messages to another. This might seem odd, but keep in mind that tool is designed in the confines of exercise environment. Some information cannot be exposed to the players, such as host names that might reveal insights into the game network layout and scenarios. Thus, original and processed messages are kept separate on cluster level. Nevertheless, messages could easily be fed back into original cluster if so configured or if output cluster configuration is omitted. 

## Subcommands

Peek viper and cobra libraries to produce a cli app that supports global config flags and subcommands with their respective flags. Majority of flags are global but may not be supported by all subcommands. Every config option can be specified as cli flag, environment variable or config file option. See `example_conf.yml` for yaml version of configuration file. Or refer to cli help command.

```
go-peek --help
```

### Run

Main subcommand that consumes messages from N sources, applies required processors, and then sends modified messages to M output producers.

### Replay

The nature of online data makes experimentation difficult. Logs can be read from files post-mortem, but this approach omits temporal properties that are critical when developing correlation rules (e.g., if event A and event B occur within interval T, output new event C or take action D). This is made even more challenging in cyber exercise environment where gameplay takes place over a course of few days and new targets are constantly being added.

To overcome this problem, peek supports offline replay mode. All known timestamp fields from all log files from configured source directories will be parsed using [simple time](/pkg/events/simpletime.go) library. Log sources will be processed sequentially while individual file reading and message parsing will be parallelized using configured worker count. Timestamp of first log message is used for ordering log files. Subtracted differences in nanoseconds between sequential log messages are appended to a linked list that will later be used to invoke `time.Sleep()`. Optional `-time-from` and `-time-to` flags can be used to specify a desired replay interval. `--cache` flag can be used to store timestamp diffs on dist, to avoid reparsing on every run, but file handles are still scanned to find correct offsets. Finally, a concurrent goroutine will be spawned for each configured stream input, whereas time-ordered log files will be read sequentially within those workers. Each worker will then pull sleep values from their respective slice and send messages any supported output module.

```
go-peek --config ~/.config/peek.yaml --trace replay --time-from "2019-01-29 08:33:31" --time-to "2019-02-02 23:35:13" --cache
```

#### Limitations

Timestamps are currently kept in process memory and replay can therefor consume significant amount of RAM. Reducing memory footprint is currently not needed for our datasets.

Furthermore, replay time accuracy is best effort. Nanosecond scale drift is acceptable for event correlation tools that usually operate in human-measured intervals (i.e., seconds, minutes, hours). Note that time difference calculation assumes sequential timestamps from log messages. It does have any corrective measures if log sequence timestamping is messed up, which can be a common thing when syslog sender host clocks are out of sync.

### Readfiles

Debug routine to simply read all log files post mortem to stdout. Supports file discovery and reading from gzipped files.

### Split

Take log files from an interval and split into N smaller log files where each file contains messages from subinterval. Written to develop replay functionality.

### Syslog

**Work in progress**. Spawn a simple UDP server on specified port and listen for RFC5424 (only IETF, not BSD format) messages. Successfully parsed messages are then formatted to structured messages that are expected by main `run` command. **Not meant to replace old-school syslog daemons.** Simply meant to be used for reference on how unstructured syslog messages can be made usable by core commands.

RFC5424 limitation is because [fast influxdb syslog parser is used](https://github.com/influxdata/go-syslog/) that does not support old BSD format messages. Rsyslog client can be configured to forward all messages to listener with following configuration, assuming port `10001` is used in testing VM.

```
*.* @192.168.33.1:10001;RSYSLOG_SyslogProtocol23Format
```

## Building from source

### Install dep

Install [dep](https://golang.github.io/dep/docs/installation.html) if you want to simplify dependency deployment. On Arch Linux, it can be done with following command.

```
pacman -Sy dep
```

Otherwise use your OS equivalent install method or build from source. Then get the dependencies and install.

```
dep ensure
go install ./.
```

## TODO
