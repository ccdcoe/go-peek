package logfile

import (
	"bufio"
	"io"

	"github.com/ccdcoe/go-peek/pkg/utils"
)

func statLogFileSinglePass(file io.Reader) (first, last []byte, lines int64, err error) {
	var (
		line  []byte
		count int64
	)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line = scanner.Bytes()
		if count == 0 {
			first = utils.DeepCopyBytes(line)
		}
		count++
	}
	last = line
	return first, last, count, scanner.Err()
}
