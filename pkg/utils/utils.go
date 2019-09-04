package utils

import (
	"encoding/gob"
	"io"
	"os"
	"os/user"
	"path/filepath"
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

func FileNotExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return true
	}
	return false
}

func StringIsValidDir(path string) bool {
	data, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !data.IsDir() {
		return false
	}
	return true
}

func ExpandHome(path string) (string, error) {
	if len(path) == 0 || path[0] != '~' {
		return path, nil
	}

	usr, err := user.Current()
	if err != nil {
		return path, err
	}
	return filepath.Join(usr.HomeDir, path[1:]), nil
}

// DeepCopyBytes does exactly what the name suggests
// written for collecting scanner.Bytes() into a channel
// passing scanner.Bytes() actually passes a pointer to something that scanner is constantly modyfing
// resulting in a data race and broken parsers
// only way to truly copy a slice is to loop over all elements
func DeepCopyBytes(in []byte) []byte {
	data := make([]byte, len(in))
	for i, b := range in {
		data[i] = b
	}
	return data
}
