package logfile

import (
	"bufio"
	"compress/gzip"
	"io"
	"net/http"
	"os"
)

// GetFileContentType attempts to read first 512bytes of a file and returns an enum value of MIME content type
func GetFileContentType(path string) (Content, error) {
	mime, err := getFileMimeContentType(path)
	if err != nil {
		return Octet, err
	}
	switch mime {
	case mime:
		return Gzip, nil
	default:
		return Octet, nil
	}
}

func getFileMimeContentType(path string) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()

	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)

	_, err = in.Read(buffer)
	if err != nil {
		return "", err
	}

	// Use the net/http package's handy DectectContentType function. Always returns a valid
	// content-type by returning "application/octet-stream" if no others seemed to match.
	contentType := http.DetectContentType(buffer)

	return contentType, nil
}

/*
	These functions allow detection of file magic without relying on http package
*/

func magic(path string) (Content, error) {
	var (
		err error
		mag []byte
		in  io.ReadCloser
	)
	if in, err = os.Open(path); err != nil {
		return Octet, err
	}
	defer in.Close()

	if mag, err = bufio.NewReader(in).Peek(8); err != nil {
		return Octet, err
	}

	switch {
	case mag[0] == 31 && mag[1] == 139:
		return Gzip, nil
	case mag[0] == 253 && mag[1] == 55 && mag[2] == 122 && mag[3] == 88 && mag[4] == 90 && mag[5] == 0 && mag[6] == 0:
		return Xz, nil
	case mag[0] == 255 && mag[1] == 254:
		return Utf16, nil
	case mag[0] == 239 && mag[1] == 187 && mag[2] == 191:
		return Utf8, nil
	default:
		return Octet, nil
	}
}

func open(path string) (io.ReadCloser, error) {
	var (
		file *os.File
		m    Content
		err  error
	)
	if m, err = magic(path); err != nil {
		return nil, err
	}
	if file, err = os.Open(path); err != nil {
		return nil, err
	}
	switch m {
	case Gzip:
		return gzip.NewReader(file)
	default:
		return file, nil
	}
}
