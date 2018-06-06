package pgreplay

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ItemsFilteredTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "pgreplay",
			Name:      "items_filtered_total",
			Help:      "Number of items filtered by start/finish range",
		},
	)
	ItemsFilterProgressFraction = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pgreplay",
			Name:      "items_filter_progress_fraction",
			Help:      "Fractional progress through filter range, assuming linear distribution",
		},
	)
	ItemsLastStreamedTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "pgreplay",
			Name:      "items_last_streamed_timestamp",
			Help:      "Timestamp of last streamed item",
		},
	)
)

func init() {
	prometheus.MustRegister(ItemsFilteredTotal)
	prometheus.MustRegister(ItemsFilterProgressFraction)
	prometheus.MustRegister(ItemsLastStreamedTimestamp)
}

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
func (s Streamer) Stream(items chan ReplayItem, rate float64) (chan ReplayItem, error) {
	if rate < 0 {
		return nil, fmt.Errorf("cannot support negative rates: %v", rate)
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

			if diff := item.Time().Sub(lastSeen); diff > 0 {
				time.Sleep(time.Duration(float64(diff) / rate))
				lastSeen = item.Time()
				ItemsLastStreamedTimestamp.Set(float64(lastSeen.Unix()))
			}

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

			ItemsFilteredTotal.Inc()
		}
	}

	out := make(chan ReplayItem, StreamFilterBufferSize)

	go func() {
		for item := range items {
			if item == nil {
				continue
			}

			if s.finish != nil {
				if item.Time().After(*s.finish) {
					break
				}

				if s.start != nil {
					ItemsFilterProgressFraction.Set(
						float64(item.Time().Sub(*s.start)) / float64((*s.finish).Sub(*s.start)),
					)
				}
			}

			out <- item
		}

		close(out)
	}()

	return out
}
