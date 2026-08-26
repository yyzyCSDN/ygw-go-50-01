package source

import (
	"context"
	"errors"
	"time"

	"cdcpipeline/internal/model"
)

// ReadBatch is the raw result of one source read: the entries themselves plus
// the position range they cover.
type ReadBatch struct {
	Entries []LogEntry
	MaxPos  uint64
}

// IsEmpty reports whether the read produced no entries.
func (b ReadBatch) IsEmpty() bool { return len(b.Entries) == 0 }

// Reader pulls bounded batches from the source log. When the downstream
// pipeline signals backpressure, the reader pauses so the source never outruns
// the target write path.
type Reader struct {
	log          SourceLog
	batchSize    int
	stats        *model.PipelineStats
	backpressure func() bool
	pauseDelay   time.Duration
}

// NewReader returns a source reader with the given batch size.
func NewReader(log SourceLog, batchSize int, stats *model.PipelineStats) *Reader {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Reader{
		log:        log,
		batchSize:  batchSize,
		stats:      stats,
		pauseDelay: 20 * time.Millisecond,
	}
}

// SetBackpressureSource installs the downstream backpressure signal. The
// reader will stop producing batches while the signal stays true.
func (r *Reader) SetBackpressureSource(fn func() bool) {
	r.backpressure = fn
}

// SetPauseDelay overrides the pause interval between reads.
func (r *Reader) SetPauseDelay(d time.Duration) {
	if d > 0 {
		r.pauseDelay = d
	}
}

// Read returns the next batch strictly after from. It blocks while
// backpressured and returns an empty batch when the log is drained.
func (r *Reader) Read(ctx context.Context, from uint64) (ReadBatch, error) {
	entries, next, err := r.log.ReadAfter(from, r.batchSize)
	if errors.Is(err, ErrNoMore) {
		select {
		case <-ctx.Done():
			return ReadBatch{}, ctx.Err()
		case <-time.After(r.pauseDelay):
			return ReadBatch{}, nil
		}
	}
	if err != nil {
		return ReadBatch{}, err
	}
	if r.stats != nil {
		r.stats.ReadCount.Add(uint64(len(entries)))
		r.stats.LastRead.Store(next)
	}
	return ReadBatch{Entries: entries, MaxPos: next}, nil
}
