package app

import (
	"fmt"

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
