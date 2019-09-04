package utils

import "testing"

const argTsFormat = "2006-01-02 15:04:05"

var intervals = [7][2]string{
	{"2019-07-19 06:01:43", "2019-07-19 07:01:43"},
	{"2019-07-19 07:01:43", "2019-07-19 08:01:43"},
	{"2019-07-19 08:01:43", "2019-07-19 09:01:43"},
	{"2019-07-19 09:01:43", "2019-07-19 10:01:43"},
	{"2019-07-19 10:01:43", "2019-07-19 11:01:43"},
	{"2019-07-19 11:01:43", "2019-07-19 12:01:43"},
	{"2019-07-19 12:01:43", "2019-07-19 13:01:43"},
}
var rng = [2]string{"2019-07-19 07:30:53", "2019-07-19 10:15:10"}
var truth = [7]bool{
	false,
	true,
	true,
	true,
	true,
	false,
	false,
}

func TestIntervalValidCheck(t *testing.T) {
	r, err := NewIntervalFromStrings(rng[0], rng[1], argTsFormat)
	if err != nil {
		t.Fatal(err)
	}
	for idx, intrv := range intervals {
		obj, err := NewIntervalFromStrings(intrv[0], intrv[1], argTsFormat)
		if err != nil {
			t.Fatal(err)
		}
		if IntervalInRange(*obj, *r) != truth[idx] {
			t.Fatalf("Interval %+v should be in %+v\n", intrv, r)
		}
	}
}
