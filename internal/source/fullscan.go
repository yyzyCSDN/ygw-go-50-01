package source

import (
	"context"

	"cdcpipeline/internal/model"
)

// SnapshotRow is one row captured by the full-table scan.
type SnapshotRow struct {
	Table string
	Row   map[string]string
}

// FullScanner performs the initial full-table snapshot used by the
// full-to-incremental handoff. The snapshot watermark is fixed at scan start
// so events appended while the scan is running are replayed by the
// incremental phase instead of being lost or duplicated.
type FullScanner struct {
	log      SourceLog
	tables   []string
	afterTable func(table string)
}

// NewFullScanner returns a scanner over the given tables.
func NewFullScanner(log SourceLog, tables []string) *FullScanner {
	return &FullScanner{log: log, tables: append([]string(nil), tables...)}
}

// SetAfterTable installs an optional hook invoked after each table snapshot.
// The production server leaves it unset; tests use it to simulate changes
// arriving mid-scan.
func (s *FullScanner) SetAfterTable(hook func(table string)) {
	s.afterTable = hook
}

// Snapshot captures all tables and returns the rows plus the start watermark
// that the incremental phase must resume from.
func (s *FullScanner) Snapshot(ctx context.Context) ([]SnapshotRow, model.Position, error) {
	start := model.Position(s.log.MaxPosition())
	var rows []SnapshotRow
	for _, table := range s.tables {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}
		snapshot, err := s.log.SnapshotRow(table)
		if err != nil {
			return nil, 0, err
		}
		for _, row := range snapshot {
			rows = append(rows, SnapshotRow{Table: table, Row: row})
		}
		if s.afterTable != nil {
			s.afterTable(table)
		}
	}
	return rows, start, nil
}

// Tables returns the tables covered by the scanner.
func (s *FullScanner) Tables() []string {
	return append([]string(nil), s.tables...)
}
