package app

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

func Throw(context string, err error) {
	if context == "" {
		context = "app"
	}
	if err != nil {
		panic(fmt.Errorf("%s: %s ", context, err))
	}
}

func Catch(logger *logrus.Logger) {
	if err := recover(); err != nil {
		switch err.(type) {
		default:
			// this is exception
			logger.Fatal(err)
		}
	}
}

func Start(command string, logger *logrus.Logger) time.Time {
	logger.WithFields(logrus.Fields{
		"at":      time.Now(),
		"command": command,
	}).Info("Starting up")
	return time.Now()
}

func Done(command string, start time.Time, logger *logrus.Logger) {
	logger.WithFields(
		logrus.Fields{
			"duration": time.Since(start),
			"command":  command,
		},
	).Info("All done!")
}

func DumpJSON(path string, data interface{}) error {
	bin, err := json.Marshal(data)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Write(bin)
	return nil
}
