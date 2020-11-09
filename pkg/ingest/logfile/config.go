package logfile

import (
	"context"
	"fmt"
	"time"

	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Paths []string

	StatFunc    StatFileIntervalFunc
	StatWorkers int

	MapFunc func(string) events.Atomic

	ConsumeWorkers int
	Ctx            context.Context
}

func (c *Config) Validate() error {
	if c.Paths == nil {
		return fmt.Errorf("File input module is missing root paths")
	}
	for _, pth := range c.Paths {
		if !utils.StringIsValidDir(pth) {
			return fmt.Errorf("%s is not a valid directory", pth)
		}
	}
	if c.StatFunc == nil {
		log.Tracef(
			"File interval stat function missing for %+v, initializing empty interval",
			c.Paths,
		)
		c.StatFunc = func(first, last []byte) (utils.Interval, error) {
			return utils.Interval{
				Beginning: time.Time{},
				End:       time.Time{},
			}, nil
		}
	}
	if c.StatWorkers < 1 {
		c.StatWorkers = 1
	}
	if c.ConsumeWorkers < 1 {
		c.ConsumeWorkers = 1
	}
	if c.Ctx == nil {
		c.Ctx = context.Background()
	}
	return nil
}
