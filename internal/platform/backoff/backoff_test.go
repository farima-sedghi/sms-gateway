package backoff

import (
	"testing"
	"time"
)

func TestExponential(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, // clamped to attempt 1
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},    // 32s capped
		{1000, 30 * time.Second}, // no overflow
	}
	for _, c := range cases {
		if got := Exponential(c.attempt, time.Second, 30*time.Second); got != c.want {
			t.Errorf("Exponential(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestEqualJitter(t *testing.T) {
	d := 10 * time.Second
	if got := EqualJitter(d, 0); got != 5*time.Second {
		t.Errorf("r=0: %v, want 5s", got)
	}
	if got := EqualJitter(d, 0.999999); got < 9*time.Second || got > d {
		t.Errorf("r~1: %v, want just under 10s", got)
	}
}
