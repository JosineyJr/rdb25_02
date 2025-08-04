package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

const (
	RingBufferSize = 1048576 // 2^20
)

type DataPoint struct {
	Timestamp int64
	Amount    float64
}

type RingBufferAggregator struct {
	defaultData        []DataPoint
	defaultWriteCursor atomic.Uint64

	fallbackData        []DataPoint
	fallbackWriteCursor atomic.Uint64
}

func NewInMemoryAggregator() (*RingBufferAggregator, error) {
	return &RingBufferAggregator{
		defaultData:  make([]DataPoint, RingBufferSize),
		fallbackData: make([]DataPoint, RingBufferSize),
	}, nil
}

func (a *RingBufferAggregator) Update(
	ctx context.Context,
	processor string,
	ts int64,
) {
	dp := DataPoint{
		Timestamp: ts,
		Amount:    19.9,
	}

	if processor == payments.DefaultProcessor {
		cursor := a.defaultWriteCursor.Add(1)
		index := (cursor - 1) & (RingBufferSize - 1)
		a.defaultData[index] = dp
	} else {
		cursor := a.fallbackWriteCursor.Add(1)
		index := (cursor - 1) & (RingBufferSize - 1)
		a.fallbackData[index] = dp
	}
}

func (a *RingBufferAggregator) GetSummary(
	ctx context.Context, from, to *time.Time,
) (payments.SummaryData, payments.SummaryData, error) {
	var defaultSummary, fallbackSummary payments.SummaryData
	fromNano, toNano := from.UnixNano(), to.UnixNano()

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		defaultCursor := a.defaultWriteCursor.Load()
		defaultCount := min(defaultCursor, RingBufferSize)

		for i := range defaultCount {
			index := (defaultCursor - 1 - i) & (RingBufferSize - 1)
			dp := &a.defaultData[index]

			if dp.Timestamp == 0 {
				continue
			}
			if dp.Timestamp >= fromNano && dp.Timestamp <= toNano {
				defaultSummary.Count++
				defaultSummary.Total += dp.Amount
			}
		}
	}()

	go func() {
		defer wg.Done()

		fallbackCursor := a.fallbackWriteCursor.Load()
		fallbackCount := min(fallbackCursor, RingBufferSize)

		for i := range fallbackCount {
			index := (fallbackCursor - 1 - i) & (RingBufferSize - 1)
			dp := &a.fallbackData[index]

			if dp.Timestamp == 0 {
				continue
			}
			if dp.Timestamp >= fromNano && dp.Timestamp <= toNano {
				fallbackSummary.Count++
				fallbackSummary.Total += dp.Amount
			}
		}
	}()

	wg.Wait()

	return defaultSummary, fallbackSummary, nil
}

func (a *RingBufferAggregator) PurgeSummary(ctx context.Context) error {
	a.defaultWriteCursor.Store(0)
	a.defaultData = make([]DataPoint, RingBufferSize)

	a.fallbackWriteCursor.Store(0)
	a.fallbackData = make([]DataPoint, RingBufferSize)

	return nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
