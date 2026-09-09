package durable

import (
	"testing"
	"time"
)

func TestManualClockAdvance(t *testing.T) {
	start := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.FixedZone("test", -3*60*60))
	clock := NewManualClock(start)

	if got := clock.Now(); !got.Equal(start) || got.Location() != time.UTC {
		t.Fatalf("clock start = %v, want the same instant in UTC", got)
	}
	clock.Advance(7 * time.Minute)
	if got, want := clock.Now(), start.UTC().Add(7*time.Minute); !got.Equal(want) {
		t.Fatalf("clock after advance = %v, want %v", got, want)
	}
}
