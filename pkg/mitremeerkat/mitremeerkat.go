package mitremeerkat

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

type Mapping struct {
	ID     string `json:"id"`
	MSG    string `json:"msg"`
	Name   string `json:"name"`
	Tactic string `json:"tactic"`
	SID    int    `json:"sid"`
}

func ParseCSV(path string) ([]Mapping, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows []Mapping
	reader := csv.NewReader(f)
	var count int
	// read first row which contains labels
	reader.Read()
loop:
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break loop
		} else if err != nil {
			return nil, err
		}
		sid, err := strconv.Atoi(row[0])
		if err != nil {
			return nil, err
		}
		rows = append(rows, Mapping{
			SID:    sid,
			Tactic: row[1],
			ID:     row[2],
			Name:   row[3],
			MSG:    row[4],
		})
		count++
	}
	return rows, nil
}
