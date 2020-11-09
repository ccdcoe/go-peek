package helpers

import (
	"fmt"
	"path/filepath"

	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type DirSource struct {
	Paths []string
	Type  events.Atomic
}

type DirSources []DirSource

func (d DirSources) MapFunc() func(string) events.Atomic {
	m := make(map[string]events.Atomic)
	for _, s := range d {
		for _, p := range s.Paths {
			m[p] = s.Type
		}
	}
	return func(pth string) events.Atomic {
		return m[filepath.Clean(pth)]
	}
}

func (d DirSources) Files() []string {
	f := make([]string, 0)
	for _, file := range d {
		f = append(f, file.Paths...)
	}
	return f
}

func GetUxSockistingFromViper() DirSources {
	var pth []string
	var err error
	paths := make(DirSources, 0)
	for _, event := range events.Atomics {
		// Early return if event type is not configured
		if pth = viper.GetStringSlice(fmt.Sprintf("stream.%s.uxsock", event)); pth == nil || len(pth) == 0 {
			log.WithFields(log.Fields{
				"type": event.String(),
			}).Trace("input not configured")
			continue
		}

		for i, p := range pth {
			if p, err = utils.ExpandHome(p); err != nil {
				log.WithFields(log.Fields{
					"path": pth,
				}).Fatal("invalid path")
			}
			pth[i] = filepath.Clean(p)
		}

		paths = append(paths, DirSource{
			Paths: pth,
			Type:  event,
		})

		log.WithFields(log.Fields{
			"type": event.String(),
			"path": pth,
		}).Debug("configured input source")
	}
	log.Tracef("found %d unix sockets", len(paths))
	return paths
}

func GetDirListingFromViper() DirSources {
	var pth []string
	var err error
	paths := make(DirSources, 0)
	for _, event := range events.Atomics {
		// Early return if event type is not configured
		if pth = viper.GetStringSlice(fmt.Sprintf("stream.%s.dir", event)); pth == nil || len(pth) == 0 {
			log.WithFields(log.Fields{
				"type": event.String(),
			}).Trace("input not configured")
			continue
		}

		for i, p := range pth {
			if p, err = utils.ExpandHome(p); err != nil || !utils.StringIsValidDir(p) {
				log.WithFields(log.Fields{
					"path": pth,
				}).Fatal("invalid path")
			}
			pth[i] = filepath.Clean(p)
		}

		paths = append(paths, DirSource{
			Paths: pth,
			Type:  event,
		})

		logContext := log.WithFields(log.Fields{
			"type":  event.String(),
			"event": "configured input source",
		})
		for _, p := range pth {
			logContext.Debug(p)
		}

	}
	log.Tracef("found %d logfile directories", len(paths))
	return paths
}
