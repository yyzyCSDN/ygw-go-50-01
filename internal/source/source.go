package source

import (
	"errors"
	"sort"
	"sync"

	"cdcpipeline/internal/model"
)

// LogEntry is one raw change captured from the source database log.
type LogEntry struct {
	Seq           uint64
	Table         string
	Op            model.OpType
	Row           map[string]string
	OldRow        map[string]string
	TxnID         string
	TxnSeq        uint64
	SchemaVersion uint64
	Raw           string
}

// ErrNoMore is returned when the log has no entries after the given position.
var ErrNoMore = errors.New("no more entries")

// SourceLog is the change stream abstraction backed by the source database.
type SourceLog interface {
	Append(entries ...LogEntry) error
	ReadAfter(pos uint64, limit int) ([]LogEntry, uint64, error)
	MaxPosition() uint64
	Tables() []string
	SnapshotRow(table string) ([]map[string]string, error)
}

// InMemoryLog is an in-process simulation of a source change log. It is used
// by the demo server and by the pipeline tests, and it is safe for concurrent
// writers.
type InMemoryLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	tables  map[string]struct{}
	snap    map[string][]map[string]string
	pos     uint64
}

// NewInMemoryLog returns an empty in-memory source log.
func NewInMemoryLog() *InMemoryLog {
	return &InMemoryLog{
		tables: make(map[string]struct{}),
		snap:   make(map[string][]map[string]string),
	}
}

// Append adds entries to the log. Entries without a sequence number are
// assigned the next position in the log.
func (l *InMemoryLog) Append(entries ...LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range entries {
		if entries[i].Seq == 0 {
			l.pos++
			entries[i].Seq = l.pos
		} else if entries[i].Seq > l.pos {
			l.pos = entries[i].Seq
		}
		l.entries = append(l.entries, entries[i])
		l.tables[entries[i].Table] = struct{}{}
	}
	return nil
}

// ReadAfter returns up to limit entries strictly after pos.
func (l *InMemoryLog) ReadAfter(pos uint64, limit int) ([]LogEntry, uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]LogEntry, 0, limit)
	next := pos
	for _, entry := range l.entries {
		if entry.Seq <= pos {
			continue
		}
		out = append(out, entry)
		next = entry.Seq
		if len(out) >= limit {
			break
		}
	}
	if len(out) == 0 {
		return nil, pos, ErrNoMore
	}
	return out, next, nil
}

// MaxPosition returns the highest position written to the log.
func (l *InMemoryLog) MaxPosition() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.pos
}

// Tables returns the sorted table names present in the log.
func (l *InMemoryLog) Tables() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	names := make([]string, 0, len(l.tables))
	for table := range l.tables {
		names = append(names, table)
	}
	sort.Strings(names)
	return names
}

// PutSnapshot stores the full-table snapshot rows for a table.
func (l *InMemoryLog) PutSnapshot(table string, rows []map[string]string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snap[table] = append([]map[string]string(nil), rows...)
}

// SnapshotRow returns the full snapshot rows captured for a table.
func (l *InMemoryLog) SnapshotRow(table string) ([]map[string]string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	rows := l.snap[table]
	return append([]map[string]string(nil), rows...), nil
}
