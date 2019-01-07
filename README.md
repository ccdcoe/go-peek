# go-peek
> Peek is a simple streaming pre-processor and enrichment tool for structured logs. 

Peek was written to be central data normalization engine for [Frankenstack](https://github.com/ccdcoe/frankenstack), Crossed Swords Cyber Defense Exercise Yellow team feedback system. Originally developed during 2018 [Frankencoding](https://github.com/ccdcoe/Frankencoding) hackathon, as a lightweight alternative to general purpose log processing tools (e.g., LogStash, Rsyslog) and custom (nasty) Python scripts used in prior exercise iterations. Each message is enriched using inventory information from targets to simplify event correlation, no normalize addressing (i.e. IPv6 short format vs long), etc.

Current version is designed to consume events from one Kafka cluster and produce transformed messages to another. This might seem odd, but keep in mind that tool is designed in the confines of exercise environment. Some information cannot be exposed to the players, such as host names that might reveal insights into the game network layout and scenarios. Thus, original and processed messages are kept separate on cluster level. Nevertheless, messages could easily be fed back into original cluster if so configured or if output cluster configuration is omitted. 

## Feedback streams

Sometimes messages need to be fed into another tool in specific format to generate a new event stream. Currently, only [sagan](https://github.com/beave/sagan) is supported. See example config for more details

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
