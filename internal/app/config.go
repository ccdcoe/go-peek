package app

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	// Kafka Input
	FlagInKafkaEnabled       = "input-kafka-enabled"
	FlagInKafkaTopics        = "input-kafka-topics"
	FlagInKafkaBrokers       = "input-kafka-brokers"
	FlagInKafkaConsumerGroup = "input-kafka-consumer-group"

	// Kafka topic mapper
	FlagInKafkaTopicMapper = "input-kafka-topic-map"
	FlagInKafkaTopicAssets = "input-kafka-topic-assets"

	// Kafka Output
	FlagOutKafkaEnabled    = "output-kafka-enabled"
	FlagOutKafkaTopic      = "output-kafka-topic"
	FlagOutKafkaBrokers    = "output-kafka-brokers"
	FlagOutKafkaTopicSplit = "output-kafka-topic-split"
	FlagOutKafkaTopicEmit  = "output-kafka-topic-emit"

	// Elastic Output
	FlagOutElasticHosts  = "output-elastic-hosts"
	FlagOutElasticPrefix = "output-elastic-prefix"

	// Logging flags
	FlagLogInterval = "log-interval"

	// Sigma flags
	FlagSigmaRulesetPaths = "sigma-ruleset-path"
)

func RegisterOutputKafka(prefix string, pFlags *pflag.FlagSet) {
	pFlags.Bool(FlagOutKafkaEnabled, false, "Enable kafka output")
	viper.BindPFlag(prefix+".output.kafka.enabled", pFlags.Lookup(FlagOutKafkaEnabled))

	pFlags.String(FlagOutKafkaTopic, "peek", "Kafka output topic")
	viper.BindPFlag(prefix+".output.kafka.topic", pFlags.Lookup(FlagOutKafkaTopic))

	pFlags.StringSlice(FlagOutKafkaBrokers, []string{"localhost:9092"}, "Kafka output broker list")
	viper.BindPFlag(prefix+".output.kafka.brokers", pFlags.Lookup(FlagOutKafkaBrokers))
}

func RegisterOutputKafkaEnrichment(prefix string, pFlags *pflag.FlagSet) {
	pFlags.Bool(FlagOutKafkaTopicSplit, false, "Split output to multiple topics per event kind. "+
		fmt.Sprintf("uses --%s as prefix.", FlagOutKafkaTopic))
	viper.BindPFlag(prefix+".output.kafka.topic_split", pFlags.Lookup(FlagOutKafkaTopicSplit))

	pFlags.String(FlagOutKafkaTopicEmit, "emit", "Kafka topic for emitting fast-tracked events.")
	viper.BindPFlag(prefix+".output.kafka.topic_emit", pFlags.Lookup(FlagOutKafkaTopicEmit))
}

func RegisterOutputElastic(prefix string, pFlags *pflag.FlagSet) {
	pFlags.StringSlice(FlagOutElasticHosts, []string{"http://localhost:9200"}, "List of elastic hosts. Needs http:// prefix.")
	viper.BindPFlag(prefix+".output.elasticsearch.hosts", pFlags.Lookup(FlagOutElasticHosts))

	pFlags.String(FlagOutElasticPrefix, "peek", "Prefix to be prepended to dynamically generated elastic index")
	viper.BindPFlag(prefix+".output.elasticsearch.prefix", pFlags.Lookup(FlagOutElasticPrefix))
}

func RegisterInputKafkaPreproc(prefix string, pFlags *pflag.FlagSet) {
	RegisterInputKafkaCore(prefix, pFlags)
	RegisterInputKafkaTopicMap(prefix, pFlags)
}

func RegisterInputKafkaGenericSimple(prefix string, pFlags *pflag.FlagSet) {
	pFlags.StringSlice(FlagInKafkaTopics, []string{}, "List of input topics")
	viper.BindPFlag(prefix+".input.kafka.topics", pFlags.Lookup(FlagInKafkaTopics))

	RegisterInputKafkaCore(prefix, pFlags)
}

func RegisterInputKafkaCore(prefix string, pFlags *pflag.FlagSet) {
	pFlags.StringSlice(FlagInKafkaBrokers, []string{"localhost:9092"}, "List of input brokers")
	viper.BindPFlag(prefix+".input.kafka.brokers", pFlags.Lookup(FlagInKafkaBrokers))

	pFlags.String(FlagInKafkaConsumerGroup, "peek", "Kafka consumer group")
	viper.BindPFlag(prefix+".input.kafka.consumer_group", pFlags.Lookup(FlagInKafkaConsumerGroup))
}

func RegisterInputKafkaTopicMap(prefix string, pFlags *pflag.FlagSet) {
	pFlags.StringSlice(FlagInKafkaTopicMapper, []string{}, "Topic and event type separated by colon")
	viper.BindPFlag(prefix+".input.kafka.topic_map", pFlags.Lookup(FlagInKafkaTopicMapper))
}

func RegisterInputKafkaEnrich(prefix string, pFlags *pflag.FlagSet) {
	pFlags.String(FlagInKafkaTopicAssets, "assets", "Topic that holds asset information")
	viper.BindPFlag(prefix+".input.kafka.topic_assets", pFlags.Lookup(FlagInKafkaTopicAssets))

	RegisterInputKafkaTopicMap(prefix, pFlags)
}

func RegisterSigmaRulesetPaths(prefix string, pFlags *pflag.FlagSet) {
	pFlags.StringSlice(FlagSigmaRulesetPaths, []string{}, "Ruleset kind and path separated by colon")
	viper.BindPFlag(prefix+".sigma.ruleset_path", pFlags.Lookup(FlagSigmaRulesetPaths))
}

func RegisterLogging(prefix string, pFlags *pflag.FlagSet) {
	pFlags.Duration(FlagLogInterval, 30*time.Second, "periodic logging and report interval")
	viper.BindPFlag(prefix+".log.interval", pFlags.Lookup(FlagLogInterval))
}
