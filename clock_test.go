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

func TestManualTickersAreDueCoalescedAndStopped(t *testing.T) {
	clock := NewManualClock(time.Unix(0, 0))
	ticker := clock.NewTicker(time.Second)
	clock.Advance(999 * time.Millisecond)
	select {
	case <-ticker.Ticks():
		t.Fatal("ticker fired early")
	default:
	}
	clock.Advance(1 * time.Millisecond)
	if tick := <-ticker.Ticks(); !tick.Equal(time.Unix(1, 0)) {
		t.Fatalf("tick = %v", tick)
	}
	clock.Advance(10 * time.Second)
	<-ticker.Ticks()
	select {
	case <-ticker.Ticks():
		t.Fatal("slow consumer received an unbounded backlog")
	default:
	}
	ticker.Stop()
	ticker.Stop()
	clock.Advance(time.Hour)
	select {
	case <-ticker.Ticks():
		t.Fatal("stopped ticker fired")
	default:
	}
}
