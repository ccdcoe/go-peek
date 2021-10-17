package app

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	// Kafka Input
	FlagInKafkaEnabled       = "input-kafka-enabled"
	FlagInKafkaTopics        = "input-kafka-topics"
	FlagInKafkaBrokers       = "input-kafka-brokers"
	FlagInKafkaConsumerGroup = "input-kafka-consumer-group"

	// Kafka Output
	FlagOutKafkaEnabled = "output-kafka-enabled"
	FlagOutKafkaTopic   = "output-kafka-topic"
	FlagOutKafkaBrokers = "output-kafka-brokers"

	// Elastic Output
	FlagOutElasticHosts  = "output-elastic-hosts"
	FlagOutElasticPrefix = "output-elastic-prefix"
)

func RegisterOutputKafka(prefix string, pFlags *pflag.FlagSet) {
	pFlags.Bool(FlagOutKafkaEnabled, false, "Enable kafka output")
	viper.BindPFlag(prefix+".output.kafka.enabled", pFlags.Lookup(FlagOutKafkaEnabled))

	pFlags.String(FlagOutKafkaTopic, "peek", "Kafka output topic")
	viper.BindPFlag(prefix+".output.kafka.topic", pFlags.Lookup(FlagOutKafkaTopic))

	pFlags.StringSlice(FlagOutKafkaBrokers, []string{"localhost:9092"}, "Kafka output broker list")
	viper.BindPFlag(prefix+".output.kafka.brokers", pFlags.Lookup(FlagOutKafkaBrokers))
}

func RegisterOutputElastic(prefix string, pFlags *pflag.FlagSet) {
	pFlags.StringSlice(FlagOutElasticHosts, []string{"http://localhost:9200"}, "List of elastic hosts. Needs http:// prefix.")
	viper.BindPFlag(prefix+".output.elasticsearch.hosts", pFlags.Lookup(FlagOutElasticHosts))

	pFlags.String(FlagOutElasticPrefix, "peek", "Prefix to be prepended to dynamically generated elastic index")
	viper.BindPFlag(prefix+".output.elasticsearch.prefix", pFlags.Lookup(FlagOutElasticPrefix))
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
