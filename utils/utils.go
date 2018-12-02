package utils

import (
	"encoding/gob"
	"io"
	"os"
)

func GobLoadFile(path string, object interface{}) error {
	var (
		err  error
		file io.ReadCloser
	)
	if file, err = os.Open(path); err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(object); err != nil {
		return err
	}
	return nil
}

func GobSaveFile(path string, object interface{}) error {
	var (
		err  error
		file io.WriteCloser
	)
	if file, err = os.Create(path); err != nil {
		return err
	}
	defer file.Close()

	var encoder = gob.NewEncoder(file)
	if err = encoder.Encode(object); err != nil {
		return err
	}
	return nil
}
