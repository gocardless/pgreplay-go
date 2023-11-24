package pgreplay

import (
	"fmt"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	itemsFilteredTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pgreplay_items_filtered_total",
			Help: "Number of items filtered by start/finish range",
		},
	)
	itemsFilterProgressFraction = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_items_filter_progress_fraction",
			Help: "Fractional progress through filter range, assuming linear distribution",
		},
	)
	_ = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pgreplay_items_last_streamed_timestamp",
			Help: "Timestamp of last streamed item",
		},
	)
)

// StreamFilterBufferSize is the size of the channel buffer when filtering items for a
// time range
var StreamFilterBufferSize = 100

type Streamer struct {
	start  *time.Time
	finish *time.Time
	logger kitlog.Logger
}

func NewStreamer(start, finish *time.Time, logger kitlog.Logger) Streamer {
	return Streamer{start, finish, logger}
}

// Stream takes all the items from the given items channel and returns a channel that will
// receive those events at a simulated given rate.
func (s Streamer) Stream(items chan Item, rate float64) (chan Item, error) {
	if rate < 0 {
		return nil, fmt.Errorf("cannot support negative rates: %v", rate)
	}

	out := make(chan Item)

	go func() {
		var first, start time.Time
		var seenItem bool

		for item := range s.Filter(items) {
			if !seenItem {
				first = item.GetTimestamp()
				start = time.Now()
				seenItem = true
			}

			elapsedSinceStart := time.Duration(rate) * time.Since(start)
			elapsedSinceFirst := item.GetTimestamp().Sub(first)

			if diff := elapsedSinceFirst - elapsedSinceStart; diff > 0 {
				time.Sleep(time.Duration(float64(diff) / rate))
			}

			level.Debug(s.logger).Log(
				"event", "queing.item",
				"sessionID", string(item.GetSessionID()),
				"user", string(item.GetUser()),
			)
			out <- item
		}

		close(out)
	}()

	return out, nil
}

// Filter takes a Item stream and filters all items that don't match the desired
// time range, along with any items that are nil. Filtering of items before our start
// happens synchronously on first call, which will block initially until matching items
// are found.
//
// This function assumes that items are pushed down the channel in chronological order.
func (s Streamer) Filter(items chan Item) chan Item {
	if s.start != nil {
		for item := range items {
			if item == nil {
				continue
			}

			if item.GetTimestamp().After(*s.start) {
				break
			}

			itemsFilteredTotal.Inc()
		}
	}

	out := make(chan Item, StreamFilterBufferSize)

	go func() {
		for item := range items {
			if item == nil {
				continue
			}

			if s.finish != nil {
				if item.GetTimestamp().After(*s.finish) {
					break
				}

				if s.start != nil {
					itemsFilterProgressFraction.Set(
						float64(item.GetTimestamp().Sub(*s.start)) / float64((*s.finish).Sub(*s.start)),
					)
				}
			}

			out <- item
		}

		close(out)
	}()

	return out
}
