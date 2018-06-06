package pgreplay

import (
	"fmt"
	"time"
)

// StreamFilterBufferSize is the size of the channel buffer when filtering items for a
// time range
var StreamFilterBufferSize = 100

type Streamer struct {
	start  *time.Time
	finish *time.Time
}

func NewStreamer(start, finish *time.Time) Streamer {
	return Streamer{start, finish}
}

// Stream takes all the items from the given items channel and returns a channel that will
// receive those events at a simulated given rate.
func (s Streamer) Stream(items chan ReplayItem, rate int) (chan ReplayItem, error) {
	if rate < 0 {
		return nil, fmt.Errorf("cannot support negative rates: %d", rate)
	}

	out := make(chan ReplayItem)

	go func() {
		var lastSeen time.Time
		var seenItem bool

		for item := range s.Filter(items) {
			if !seenItem {
				lastSeen = item.Time()
				seenItem = true
			}

			time.Sleep(item.Time().Sub(lastSeen) / time.Duration(rate))
			out <- item
		}

		close(out)
	}()

	return out, nil
}

// Filter takes a ReplayItem stream and filters all items that don't match the desired
// time range, along with any items that are nil. Filtering of items before our start
// happens synchronously on first call, which will block initially until matching items
// are found.
//
// This function assumes that items are pushed down the channel in chronological order.
func (s Streamer) Filter(items chan ReplayItem) chan ReplayItem {
	if s.start != nil {
		for item := range items {
			if item == nil {
				continue
			}

			if item.Time().After(*s.start) {
				break
			}
		}
	}

	out := make(chan ReplayItem, StreamFilterBufferSize)

	go func() {
		for item := range items {
			if item == nil {
				continue
			}

			if s.finish != nil && item.Time().After(*s.finish) {
				break
			}

			out <- item
		}

		close(out)
	}()

	return out
}
