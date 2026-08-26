package sink_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/sink"
	"cdcpipeline/internal/source"
)

func TestSinkTimeoutBackpressure(t *testing.T) {
	target := sink.NewInMemorySink(nil)
	target.SetWriteDelay(50 * time.Millisecond)
	target.SetWriteTimeout(10 * time.Millisecond)
	event := model.ChangeEvent{
		Seq:     1,
		Table:   "t",
		Key:     "1",
		Op:      model.OpInsert,
		Columns: []model.ColumnValue{{Name: "id", Value: "1"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := target.Write(ctx, event)
	if !errors.Is(err, sink.ErrWriteTimeout) {
		t.Fatalf("slow write must time out, got %v", err)
	}
	if !target.Backpressured() {
		t.Fatalf("timeout must raise the backpressure flag")
	}
}

func TestReaderPausesUnderBackpressure(t *testing.T) {
	log := source.NewInMemoryLog()
	for i := 1; i <= 20; i++ {
		_ = log.Append(source.LogEntry{
			Seq:   uint64(i),
			Table: "t",
			Op:    model.OpInsert,
			Row:   map[string]string{"id": "1"},
		})
	}
	reader := source.NewReader(log, 5, model.NewStats())
	reader.SetBackpressureSource(func() bool { return true })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	batch, err := reader.Read(ctx, 0)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected read error: %v", err)
	}
	if !batch.IsEmpty() {
		t.Fatalf("reader must pause while the target is backpressured, got %d events", len(batch.Entries))
	}
}
