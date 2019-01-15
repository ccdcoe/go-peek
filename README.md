# go-peek
> Peek is a simple streaming pre-processor and enrichment tool for structured logs. 

Peek was written to be central data normalization engine for [Frankenstack](https://github.com/ccdcoe/frankenstack), Crossed Swords Cyber Defense Exercise Yellow team feedback system. Originally developed during 2018 [Frankencoding](https://github.com/ccdcoe/Frankencoding) hackathon, as a lightweight alternative to general purpose log processing tools (e.g., LogStash, Rsyslog) and custom (nasty) Python scripts used in prior exercise iterations. Each message is enriched using inventory information from targets to simplify event correlation, no normalize addressing (i.e. IPv6 short format vs long), etc.

Current version is designed to consume events from one Kafka cluster and produce transformed messages to another. This might seem odd, but keep in mind that tool is designed in the confines of exercise environment. Some information cannot be exposed to the players, such as host names that might reveal insights into the game network layout and scenarios. Thus, original and processed messages are kept separate on cluster level. Nevertheless, messages could easily be fed back into original cluster if so configured or if output cluster configuration is omitted. 

## Feedback streams

Sometimes messages need to be fed into another tool in specific format to generate a new event stream. Currently, only [sagan](https://github.com/beave/sagan) is supported. See example config for more details

## Replay mode

The nature of online data makes experimentation difficult. Logs can be read from files post-mortem, but this approach omits temporal properties that are critical when developing correlation rules (e.g., if event A and event B occur within interval T, output new event C or take action D). This is made even more challenging in cyber exercise environment where gameplay takes place over a course of few days and new targets are constantly being added.

To overcome this problem, peek supports offline replay mode. All known timestamp fields from all log files from configured source directories will be parsed using [simple time](/pkg/events/simpletime.go) library. Log sources will be processed sequentially while individual file reading and message parsing will be parallelized using configured worker count. Timestamp of first log message is used for ordering log files. Subtracted differences in nanoseconds between sequential log messages are appended to a linked list that will later be used to invoke `time.Sleep()`. Optional `-time-from` and `-time-to` flags can be used to specify a desired replay interval. Messages outside this interval will be discarded. Finally, a concurrent goroutine will be spawned for each configured stream input, whereas time-ordered log files will be read sequentially within those workers. Each worker will then pull sleep values from their respective linked list and copy messages to common [output messanger](/internal/types/message.go). Output can be processed as any online stream. `-ff` flag can be used to optionally apply speedup multiplier to this lost.

```
peek --config ./config.toml replay --time-from "2018-12-06 10:00:00" --time-to "2018-12-06 13:00:00" -ff 1 -stdout
```

### Limitations

Current implementation does not store parsed timestamps, and thus timestamps will be reparsed on each run. Furthermore, these linked lists are currently kept in process memory and replay can therefor consume significant amount of RAM. Reducing memory footprint is currently not needed for our datasets.

Furthermore, replay time accuracy is best effort. Nanosecond scale drift is acceptable for event correlation tools that usually operate in human-measured intervals (i.e., seconds, minutes, hours). Note that time difference calculation assumes sequential timestamps from log messages. It does have any corrective measures if log sequence timestamping is messed up, which can be a common thing when syslog sender host clocks are out of sync.

## Building from source

### Install dep

Install [dep](https://golang.github.io/dep/docs/installation.html) if you want to simplify dependency deployment. On Arch Linux, it can be done with following command.

```
pacman -Sy dep
```

Otherwise use your OS equivalent install method or build from source. Then get the dependencies and install.

```
dep ensure
go install ./cmd/...
```

## Getting started

Peek uses toml format in configuration file in combination with command line arguments. Please refer to `help` subcommand for reference.

```
peek help
```

Example configuration file can be generated using `example-config` subcommand

```
peek example-config > config.toml
```

Please modify the config as needed and then load it with `--config` main flag.

```
peek --config config.toml
```

## Testing

Peek requires Kafka and Elasticsearch to function and to run unit tests (wip). Docker compose files are provided in `build` directory to get a minimal testing environment up and running. Ensure proper `vm.max_map_count` value, otherwise Elasticsearch is liable to crash on startup.

```
cd build/
sudo sysctl -w vm.max_map_count=262144
sudo docker-compose up
```

Example messages can be generated and consumed with python tools in `scripts` directory. Assuming a recent python version, use following commands to install required packages.

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then produce and consume messages to verify output.

```
python scripts/simple-kafka-producer.py --topic syslog
```

```
python scripts/simple-kafka-consumer.py --consume-topics syslog
```

Multiple topics can be consumed by listing topic names

```
python scripts/simple-kafka-consumer.py --consume-topics syslog syslog-sagan
```

Peek requires inventory data in order to add meta information on shipper, source and destinaiton hosts, depending on log type. Currently only saltstack grain data as Elasticsearch JSON documents is supported. Example data can be found in `tests` folder. A script is provided to push that into local Elasticsearch instance.

```
python scripts/simple-inventory-setup.py
```

## Todo

* A proper todo list
