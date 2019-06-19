package decoder

import (
	"fmt"

	"github.com/ccdcoe/go-peek/pkg/utils"
)

type ReplayCache struct {
	Data TimeSliceSequenceMap
	utils.Interval
}

func StoreReplayDump(
	data TimeListSequenceMap,
	config LogReplayWorkerConfig,
	path string,
) error {
	if data == nil || len(data) == 0 {
		return fmt.Errorf("Cannot dump missing or empty map for replay")
	}
	return utils.GobSaveFile(path, ReplayCache{
		Data: data.Transform(),
		Interval: utils.Interval{
			Beginning: config.From,
			End:       config.To,
		},
	})
}

type ReplayCacheLL struct {
	Data TimeListSequenceMap
	utils.Interval
}

func LoadReplayDump(path string) (*ReplayCacheLL, error) {
	var load ReplayCache
	if err := utils.GobLoadFile(path, &load); err != nil {
		return nil, err
	}
	ll := &ReplayCacheLL{
		Data:     load.Data.Transform(),
		Interval: load.Interval,
	}
	return ll, nil
}
