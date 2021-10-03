package app

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	FlagOutKafkaEnabled = "output-kafka-enabled"
	FlagOutKafkaTopic   = "output-kafka-topic"
	FlagOutKafkaBrokers = "output-kafka-brokers"
)

func RegisterOutputKafka(prefix string, pFlags *pflag.FlagSet) {
	pFlags.Bool(FlagOutKafkaEnabled, false, "Kafka output topic")
	viper.BindPFlag(prefix+".output.kafka.enabled", pFlags.Lookup(FlagOutKafkaEnabled))

	pFlags.String(FlagOutKafkaTopic, "peek", "Kafka output topic")
	viper.BindPFlag(prefix+".output.kafka.topic", pFlags.Lookup(FlagOutKafkaTopic))

	pFlags.StringSlice(FlagOutKafkaBrokers, []string{"localhost:9092"}, "Kafka output broker list")
	viper.BindPFlag(prefix+".output.kafka.brokers", pFlags.Lookup(FlagOutKafkaBrokers))
}
