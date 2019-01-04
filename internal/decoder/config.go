package decoder

import (
	"github.com/ccdcoe/go-peek/internal/logging"
	"github.com/ccdcoe/go-peek/internal/types"
)

type DecoderConfig struct {
	// Input stream as go channel
	Input types.Messager

	// Dir for persistence etc
	Spooldir string

	// Map of topic name to parser type
	// For event.Event type switch
	// Currently requires apriori knowlede if incoming data
	EventMap map[string]string

	// Number of parallel parsers/enrichers to spawn
	// Will consume from and output to the same channel
	Workers int

	// Inventory for rename/remap
	// Only elasticsearch input is currently supported
	InventoryConfig *types.ElaTargetInventoryConfig

	// Optionally ignore SigInt
	// May be better to do signal handling externally
	// By default, sigint should stop decoder
	IgnoreSigInt bool

	// Interface for collecting async messages
	// Should be created if not given by user
	// logs from multiple modules can be collected with single object
	logging.LogHandler
}
