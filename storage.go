package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/slog"
)

type Batcher struct {
	conn *pgx.Conn
	size int

	eventsReceived *Counter

	mu     sync.Mutex
	traces map[EventType][]*TraceEvent
	count  int
}

func NewBatcher(conn *pgx.Conn, size int) (*Batcher, error) {
	b := &Batcher{
		conn:   conn,
		size:   size,
		traces: make(map[EventType][]*TraceEvent),
	}

	er, err := NewDimensionlessCounter("events_received", "Number of events received, tagged by type", eventTypeTag)
	if err != nil {
		return nil, fmt.Errorf("new gauge: %w", err)
	}
	b.eventsReceived = er

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
	return
}

func (b *Batcher) flush(ctx context.Context) error {
	// assumes mutex is held by caller
	for evtype, evs := range b.traces {
		logger := slog.With("event_type", evtype.Key(), "count", len(evs))

		tbl, ok := eventDefs[evtype]
		if !ok {
			logger.Log(slog.LevelError, "skipping unknown event type")
			continue
		}

		if tbl.BatchInsert == nil {
			logger.Warn("skipping unhandled event type")
			continue
		}

		batch, err := tbl.BatchInsert(ctx, evs)
		if err != nil {
			logger.Error("failed to create insert batch", err, "event_type")
			return fmt.Errorf("commit transaction: %w", err)
		}

		logger.Debug("persisting events")
		if err := b.execBatch(ctx, batch); err != nil {
			logger.Error("batch failed", err)
		}
	}

	b.traces = make(map[EventType][]*TraceEvent)
	b.count = 0
	return nil
}

func (b *Batcher) execBatch(ctx context.Context, batch *pgx.Batch) error {
	tx, err := b.conn.Begin(ctx)
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
