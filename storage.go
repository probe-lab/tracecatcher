package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/exp/slog"
)

type Batcher struct {
	pool *pgxpool.Pool
	size int

	eventsReceived *Counter
	eventsWritten  *Counter
	eventsDropped  *Counter
	batchAttempts  *Counter
	batchErrors    *Counter

	mu     sync.Mutex
	traces map[EventType][]*TraceEvent
	count  int
}

func NewBatcher(pool *pgxpool.Pool, size int) (*Batcher, error) {
	b := &Batcher{
		pool:   pool,
		size:   size,
		traces: make(map[EventType][]*TraceEvent),
	}

	var err error
	b.eventsReceived, err = NewDimensionlessCounter("events_received", "Number of events received, tagged by type", eventTypeTag)
	if err != nil {
		return nil, fmt.Errorf("events_received counter: %w", err)
	}
	b.eventsWritten, err = NewDimensionlessCounter("events_written", "Number of events written to database, tagged by type", eventTypeTag)
	if err != nil {
		return nil, fmt.Errorf("events_written counter: %w", err)
	}
	b.eventsDropped, err = NewDimensionlessCounter("events_dropped", "Number of events not written to database, tagged by type", eventTypeTag)
	if err != nil {
		return nil, fmt.Errorf("events_dropped counter: %w", err)
	}
	b.batchAttempts, err = NewDimensionlessCounter("batch_attempts", "Number of batch writes attempted, tagged by type", eventTypeTag)
	if err != nil {
		return nil, fmt.Errorf("batch_attempts counter: %w", err)
	}
	b.batchErrors, err = NewDimensionlessCounter("batch_errors", "Number of errors encountered while writing a batch, tagged by type", eventTypeTag)
	if err != nil {
		return nil, fmt.Errorf("batch_errors counter: %w", err)
	}

	return b, err
}

func (b *Batcher) Add(ctx context.Context, e *TraceEvent) {
	if e.Type == nil {
		slog.Warn("trace event had no type, dropping")
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.traces[*e.Type] = append(b.traces[*e.Type], e)
	b.count++

	mctx := eventTypeContext(ctx, e.Type.Key())
	b.eventsReceived.Add(mctx, 1)

	if b.count >= b.size {
		if err := b.flush(ctx); err != nil {
			slog.Error("failed to flush", err)
			return
		}
	}
}

func (b *Batcher) flush(ctx context.Context) error {
	// assumes mutex is held by caller
	for evtype, evs := range b.traces {
		logger := slog.With("event_type", evtype.Key(), "count", len(evs))
		mctx := eventTypeContext(ctx, evtype.Key())

		tbl, ok := eventDefs[evtype]
		if !ok {
			b.eventsDropped.Add(mctx, int64(len(evs)))
			logger.Log(slog.LevelError, "skipping unknown event type")
			continue
		}

		if tbl.BatchInsert == nil {
			b.eventsDropped.Add(mctx, int64(len(evs)))
			logger.Warn("skipping unhandled event type")
			continue
		}

		b.batchAttempts.Add(mctx, 1)
		batch, batchSize, err := tbl.BatchInsert(ctx, evs)
		if err != nil {
			b.batchErrors.Add(mctx, 1)
			b.eventsDropped.Add(mctx, int64(len(evs)))
			logger.Error("failed to create insert batch", err)
			continue
		}

		if batchSize < len(evs) {
			b.eventsDropped.Add(mctx, int64(len(evs)-batchSize))
		}

		logger.Debug("persisting events", "batch_size", batchSize, "dropped", len(evs)-batchSize)
		if err := b.execBatch(ctx, batch); err != nil {
			b.batchErrors.Add(mctx, 1)
			b.eventsDropped.Add(mctx, int64(batchSize))
			logger.Error("exec batch failed", err)
			continue
		}
		b.eventsWritten.Add(mctx, int64(batchSize))
	}

	b.traces = make(map[EventType][]*TraceEvent)
	b.count = 0
	return nil
}

func (b *Batcher) execBatch(ctx context.Context, batch *pgx.Batch) error {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	br := tx.SendBatch(ctx, batch)

	if err := br.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
