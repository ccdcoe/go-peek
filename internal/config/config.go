package config

type Config struct {
	General *GeneralConfig

	Errors, Notifications *LogConfig

	Kafka   *KafkaConfig
	Elastic *ElaConfig

	EventTypes map[string]*StreamConfig
}

type GeneralConfig struct {
	// Working directory where persistence files are being kept
	Spooldir string

	// Number of async goroutines for decoding messages
	// All workers shall consume from and output to the same channel
	// Uint, so we don't need to check for negative numbers
	Workers uint
}

type LogConfig struct {
	// Enable or disable logging
	Enable bool

	// Log every n-th messages
	// Useful for things such as parse errors in high-throughput message queues
	Sample int
}

type KafkaConfig struct {
	// Allows messages to be consumed from one cluster and produced to another
	// Due to our setup where we have separate clustes for middleware and exposing to frontend
	Input  []string
	Output []string

	// ConsumerGroup used for committing offsets
	ConsumerGroup string
}

type ElaConfig struct {
	// List of elasticsearch proxy hosts in http://addr:port format
	// Genaral config for outputting messages into timestamped indices
	Output []string

	// For storing anonymized names to actual name and ip to common name mappings
	// Separate from main output config, as this mapping should not be exposed to players
	// Needed by other tools
	RenameMap *ElaRenameMapConfig

	// Inventory that contains full information about hosts and targets
	// Separate, as info can be used by green team as well
	Inventory *ElaInventoryConfig
}

type ElaRenameMapConfig struct {
	// List of elasticsearch proxy hosts in http://addr:port format
	Hosts []string

	// Indices for storing host to pretty name and known ip to pretty name mappings
	HostIndex string
	AddrIndex string
}

type ElaInventoryConfig struct {
	// List of elasticsearch proxy hosts in http://addr:port format
	Hosts []string

	// Index containing inventory from salt grains
	GrainIndex string
}

type StreamConfig struct {
	// Type of messages in stream
	// To separate event.Event objects
	Type string

	// Output topic, elastic index name, etc
	// Timestamp should be appended to elastic index programmatically
	Topic string
}
