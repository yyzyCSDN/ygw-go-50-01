package model

import "sync/atomic"

// PipelineStats aggregates counters exposed by the monitor page.
type PipelineStats struct {
	ReadCount      atomic.Uint64
	ParsedCount    atomic.Uint64
	MappedCount    atomic.Uint64
	DeliveredCount atomic.Uint64
	AckedCount     atomic.Uint64
	RetryCount     atomic.Uint64
	DupSuppressed  atomic.Uint64
	Backlog        atomic.Uint64
	LastCommitted  atomic.Uint64
	LastRead       atomic.Uint64
}

// NewStats returns an empty stats accumulator.
func NewStats() *PipelineStats { return &PipelineStats{} }

// Snapshot returns a flat map of all counters for the monitor page.
func (s *PipelineStats) Snapshot() map[string]uint64 {
	return map[string]uint64{
		"read_count":      s.ReadCount.Load(),
		"parsed_count":    s.ParsedCount.Load(),
		"mapped_count":    s.MappedCount.Load(),
		"delivered_count": s.DeliveredCount.Load(),
		"acked_count":     s.AckedCount.Load(),
		"retry_count":     s.RetryCount.Load(),
		"dup_suppressed":  s.DupSuppressed.Load(),
		"backlog":         s.Backlog.Load(),
		"last_committed":  s.LastCommitted.Load(),
		"last_read":       s.LastRead.Load(),
	}
}
