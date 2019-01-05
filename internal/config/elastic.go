package config

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
