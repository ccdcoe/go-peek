package mitremeerkat

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

type Mappings []Mapping

func (m Mappings) CSVFormat(header bool) [][]string {
	tx := make([][]string, 0, len(m)+1)
	if header {
		tx = append(tx, Mapping{}.Keys())
	}
	for _, item := range m {
		tx = append(tx, item.StrSlc())
	}
	return tx
}

func NewMappings(m map[int]string) Mappings {
	tx := make(Mappings, 0)
	if m == nil || len(m) == 0 {
		return tx
	}
	for sid, msg := range m {
		tx = append(tx, Mapping{
			SID: sid,
			MSG: msg,
		})
	}
	return tx
}

type Mapping struct {
	ID     string `json:"id"`
	MSG    string `json:"msg"`
	Name   string `json:"name"`
	Tactic string `json:"tactic"`
	SID    int    `json:"sid"`
}

// Keys is for implementing CSV header
func (m Mapping) Keys() []string {
	return []string{"sid", "tactic", "id", "name", "msg"}
}

// StrSlc is for implementing CSV rows
func (m Mapping) StrSlc() []string {
	return []string{strconv.Itoa(m.SID), m.Tactic, m.ID, m.Name, m.MSG}
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
