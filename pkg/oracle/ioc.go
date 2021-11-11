package oracle

import "time"

type IndicatorOfCompromise int

// IoC stands for Indicator of compromise
type IoC struct {
	Value string
	Type  string
	Added time.Time
}
