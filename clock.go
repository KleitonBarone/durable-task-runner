package durable

import (
	"sync"
	"time"
)

// Clock controls runtime timers and injected test time. SystemClock uses
// PostgreSQL time for durable mutations; ManualClock controls exact boundaries.
type Clock interface {
	Now() time.Time
}

// TickerClock also controls runtime waiting. Clocks that only supply Now keep
// using real tickers; ManualClock supplies both for deterministic loop tests.
type TickerClock interface {
	Clock
	NewTicker(time.Duration) Ticker
}

type Ticker interface {
	Ticks() <-chan time.Time
	Stop()
}

type systemTicker struct{ *time.Ticker }

func (t systemTicker) Ticks() <-chan time.Time { return t.C }

func newTicker(clock Clock, interval time.Duration) Ticker {
	if clock, ok := clock.(TickerClock); ok {
		return clock.NewTicker(interval)
	}
	return systemTicker{time.NewTicker(interval)}
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type ManualClock struct {
	mu      sync.RWMutex
	now     time.Time
	tickers map[*manualTicker]struct{}
}

func NewManualClock(now time.Time) *ManualClock {
	return &ManualClock{now: now.UTC(), tickers: make(map[*manualTicker]struct{})}
}

func (c *ManualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for ticker := range c.tickers {
		if !ticker.next.After(c.now) {
			select {
			case ticker.ticks <- ticker.next:
			default:
			}
			ticker.next = c.now.Add(ticker.interval - c.now.Sub(ticker.next)%ticker.interval)
		}
	}
}

// NewTicker follows time.Ticker's buffered, coalescing behavior. Advancing time
// makes due ticks available without waiting for their consumers to run.
func (c *ManualClock) NewTicker(interval time.Duration) Ticker {
	if interval <= 0 {
		panic("ticker interval must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ticker := &manualTicker{clock: c, interval: interval, next: c.now.Add(interval), ticks: make(chan time.Time, 1)}
	c.tickers[ticker] = struct{}{}
	return ticker
}

type manualTicker struct {
	clock    *ManualClock
	interval time.Duration
	next     time.Time
	ticks    chan time.Time
}

func (t *manualTicker) Ticks() <-chan time.Time { return t.ticks }

func (t *manualTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	delete(t.clock.tickers, t)
}
