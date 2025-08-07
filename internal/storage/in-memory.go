package storage

import (
	"context"
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

	fallbackData []DataPoint
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

	cursor := a.defaultWriteCursor.Add(1)
	index := (cursor - 1) & (RingBufferSize - 1)
	a.defaultData[index] = dp
}

func (a *RingBufferAggregator) GetSummary(
	ctx context.Context, from, to *time.Time,
) (payments.SummaryData, payments.SummaryData, error) {
	var defaultSummary, fallbackSummary payments.SummaryData
	fromNano, toNano := from.Unix(), to.Unix()

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

	return defaultSummary, fallbackSummary, nil
}

func (a *RingBufferAggregator) PurgeSummary(ctx context.Context) error {
	a.defaultWriteCursor.Store(0)
	a.defaultData = make([]DataPoint, RingBufferSize)

	return nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
