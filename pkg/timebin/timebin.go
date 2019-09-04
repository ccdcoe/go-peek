package timebin

import (
	"math"
	"time"

	"github.com/ccdcoe/go-peek/pkg/utils"
)

// TimeBin implements timeseries binning
// in other words, splitting a period into N equally sized groups
// provides helper methods for estimating size,
type Container struct {
	List  []time.Time
	Size  time.Duration
	Count int

	utils.Interval
}

func New(interval utils.Interval, bucketSize time.Duration) (*Container, error) {
	if err := interval.Validate(); err != nil {
		return nil, err
	}
	count := int(math.Ceil(interval.Period().Seconds() / bucketSize.Seconds()))
	return &Container{
		Count:    count,
		Interval: interval,
		Size:     bucketSize,
		List:     calculateBounds(interval.Beginning, interval.End, bucketSize),
	}, nil
}

func (t Container) Locate(ts time.Time) int {
	return getBin(ts, t.End, t.Size, t.Count) - 1
}

func getBin(ts, upper time.Time, interval time.Duration, max int) int {
	return max - int(upper.Sub(ts).Nanoseconds()/interval.Nanoseconds())
}

func calculateBounds(from, to time.Time, interval time.Duration) []time.Time {
	times := make([]time.Time, 0)
	t := from
	for !t.After(to) {
		times = append(times, t)
		t = t.Add(interval)
	}
	return times
}
