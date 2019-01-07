package config

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
