package file

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
)

func FileMagic(path string) (string, error) {
	var (
		err  error
		mag  []byte
		file io.ReadCloser
	)
	if file, err = os.Open(path); err != nil {
		return "", err
	}
	defer file.Close()

	if mag, err = bufio.NewReader(file).Peek(8); err != nil {
		return "", err
	}

	switch {
	case mag[0] == 31 && mag[1] == 139:
		return "gzip", nil
	case mag[0] == 253 && mag[1] == 55 && mag[2] == 122 && mag[3] == 88 && mag[4] == 90 && mag[5] == 0 && mag[6] == 0:
		return "xz", nil
	case mag[0] == 255 && mag[1] == 254:
		return "utf16", nil
	case mag[0] == 239 && mag[1] == 187 && mag[2] == 191:
		return "utf8", nil
	default:
		return "plain", nil
	}
}

func OpenFile(path string) (io.ReadCloser, error) {
	var (
		file  io.ReadCloser
		magic string
		err   error
	)
	if file, err = os.Open(path); err != nil {
		return nil, err
	}
	if magic, err = FileMagic(path); err != nil {
		return nil, err
	}
	switch magic {
	case "gzip":
		if file, err = gzip.NewReader(file); err != nil {
			return nil, err
		}
	default:
		// TODO! Return nil, err upon unknown file type
	}

	return file, nil
}
