package main

import (
	"sort"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/sink"
)

// StatusPayload is the JSON document served to the monitor page.
type StatusPayload struct {
	Phase          string            `json:"phase"`
	Committed      uint64            `json:"committed"`
	Checkpoint     uint64            `json:"checkpoint"`
	ReadWatermark  uint64            `json:"read_watermark"`
	Backlog        uint64            `json:"backlog"`
	Backpressured  bool              `json:"backpressured"`
	PendingGroups  int               `json:"pending_groups"`
	SchemaChanges  int               `json:"schema_changes"`
	Delivery       map[string]int    `json:"delivery"`
	PendingDDL     []string          `json:"pending_ddl"`
	Stats          map[string]uint64 `json:"stats"`
	Tables         map[string]int    `json:"tables"`
	SchemaVersions map[string][]uint64 `json:"schema_versions"`
	MappingDigest  string            `json:"mapping_digest"`
}

// Status assembles the current pipeline state for the monitor.
func (s *Server) Status() StatusPayload {
	checkpoint := s.store.Snapshot()
	backlog := uint64(0)
	if uint64(checkpoint.Read) > uint64(checkpoint.Committed) {
		backlog = uint64(checkpoint.Read) - uint64(checkpoint.Committed)
	}
	tables := make(map[string]int)
	for _, table := range s.changeLog.Tables() {
		tables[table] = s.target.RowCount(table)
	}
	versions := make(map[string][]uint64)
	var pendingDDL []string
	for _, table := range s.registry.TableNames() {
		versions[table] = s.registry.Versions(table)
		if schema.VersionChanged(s.registry, table, model.Position(checkpoint.Committed), model.Position(checkpoint.Read)) {
			pendingDDL = append(pendingDDL, table)
		}
	}
	delivery := make(map[string]int)
	for _, record := range s.runner.Dispatcher().StatusSnapshot(1000) {
		delivery[record.Status.String()]++
	}
	return StatusPayload{
		Phase:          string(s.runner.Phase()),
		Committed:      uint64(checkpoint.Committed),
		Checkpoint:     s.runner.CheckpointPos(),
		ReadWatermark:  uint64(checkpoint.Read),
		Backlog:        backlog,
		Backpressured:  s.target.Backpressured(),
		PendingGroups:  s.runner.Dispatcher().PendingCount(),
		SchemaChanges:  schemaChangeCount(s.registry),
		Delivery:       delivery,
		PendingDDL:     pendingDDL,
		Stats:          s.stats.Snapshot(),
		Tables:         tables,
		SchemaVersions: versions,
		MappingDigest:  s.mapperInstance.MappingDigest(),
	}
}

// tableNames returns the configured tables in sorted order.
func tableNames(tables []string) []string {
	out := append([]string(nil), tables...)
	sort.Strings(out)
	return out
}

// schemaChangeCount reports how many schema versions exist across all tables.
func schemaChangeCount(registry *schema.Registry) int {
	count := 0
	for _, table := range registry.TableNames() {
		count += len(registry.Versions(table))
	}
	return count
}

// deliveredSummary summarizes applied rows for the monitor page.
func deliveredSummary(target *sink.InMemorySink, tables []string) map[string]int {
	summary := make(map[string]int)
	for _, table := range tables {
		summary[table] = target.RowCount(table)
	}
	return summary
}

// latestPhase is a helper used by the status endpoint to describe transitions.
func latestPhase(phase model.SyncPhase) string { return string(phase) }
