package utils

import (
	"fmt"
	"time"
)

type ErrInvalidInterval struct {
	Interval
	Src string
}

func (e ErrInvalidInterval) Error() string {
	return fmt.Sprintf(
		"Invalid interval %s: Beginning %s, End %s",
		e.Src,
		e.Interval.Beginning,
		e.Interval.End,
	)
}

func (e *ErrInvalidInterval) SetSrc(src string) *ErrInvalidInterval {
	e.Src = src
	return e
}

type Interval struct {
	Beginning, End time.Time
}

func NewIntervalFromStrings(start, stop, format string) (*Interval, error) {
	from, err := time.Parse(format, start)
	if err != nil {
		return nil, err
	}
	to, err := time.Parse(format, stop)
	if err != nil {
		return nil, err
	}
	i := &Interval{
		Beginning: from,
		End:       to,
	}
	return i, i.Validate()
}

func (i Interval) Unpack() (time.Time, time.Time) {
	return i.Beginning, i.End
}

func (i Interval) Validate() error {
	if i.End.Before(i.Beginning) {
		return &ErrInvalidInterval{
			Interval: i,
		}
	}
	return nil
}

func (i Interval) Period() time.Duration {
	return i.End.Sub(i.Beginning)
}

type IntervalInRangeFunc func(Interval, Interval) bool

func IntervalContains(i, i2 Interval) bool {
	return (i2.Beginning.After(i.Beginning) && i2.Beginning.Before(i.End)) && (i2.End.After(i.Beginning) && i2.End.Before(i.End))
}

func IntervalInRange(i, rng Interval) bool {
	return IntervalFullyInRange(i, rng) || IntervalHeadInRange(i, rng) || IntervalTailInRange(i, rng) || IntervalContains(i, rng)
}

func IntervalFullyInRange(i, rng Interval) bool {
	return TimeFullyInRange(i.Beginning, rng) && TimeFullyInRange(i.End, rng)
}

func IntervalTailInRange(i, rng Interval) bool {
	return i.End.After(rng.Beginning) && i.Beginning.Before(rng.End) && i.Beginning.Before(rng.Beginning)
}

func IntervalHeadInRange(i, rng Interval) bool {
	return i.Beginning.Before(rng.End) && i.Beginning.After(rng.Beginning) && i.End.After(rng.End)
}

// TimeInRange checks if timestamp t belongs to range rng
func TimeFullyInRange(t time.Time, rng Interval) bool {
	return t.After(rng.Beginning) && t.Before(rng.End)
}

type StatFileIntervalFunc func(first, last []byte) (Interval, error)
