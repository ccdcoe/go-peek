package file

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

type LineCountFunc func(io.Reader) (int64, error)

func CountLinesScan(file io.Reader) (int64, error) {
	fileScanner := bufio.NewScanner(file)
	var count int64
	for fileScanner.Scan() {
		count++
	}
	if count == 0 {
		return count, io.EOF
	}
	return count, nil
}

func CountLinesBlock(r io.Reader) (int64, error) {
	var (
		count   int64
		buf     = make([]byte, 32*1024)
		lineSep = []byte{'\n'}
	)

	for {
		c, err := r.Read(buf)
		count += int64(bytes.Count(buf[:c], lineSep))

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

type LineGetterFunc func(io.Reader) ([]byte, error)

func FileReadLastLine(r io.Reader) ([]byte, error) {
	var (
		buf     = make([]byte, 32*1024)
		lineSep = []byte{'\n'}
	)

	for {
		_, err := r.Read(buf)
		switch {
		case err == io.EOF:
			lines := bytes.Split(buf, lineSep)
			return lines[len(lines)-2], nil

		case err != nil:
			return []byte{}, err
		}
	}
}

func FileReadFirstLine(file io.Reader) ([]byte, error) {
	var (
		buf = make([]byte, 32*1024)
		err error
	)
	if _, err = file.Read(buf); err != nil {
		if err == io.EOF {
			return []byte{}, io.EOF
		}
		return nil, err
	}
	chunks := bytes.Split(buf, []byte("\n"))
	if len(chunks) < 1 {
		return nil, fmt.Errorf("unable to get line from buffer")
	}

	return chunks[0], nil
}
