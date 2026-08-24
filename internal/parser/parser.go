package parser

import (
	"fmt"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/source"
)

// Parser converts raw source log entries into change events. Every event is
// decoded with the schema version that was active at its source position, so a
// DDL applied later cannot corrupt events written before it.
type Parser struct {
	reg        *schema.Registry
	splitter   *TxnSplitter
	decoder    *RowDecoder
}

// NewParser returns a parser bound to the schema registry.
func NewParser(reg *schema.Registry, maxTxnSize int) *Parser {
	if maxTxnSize <= 0 {
		maxTxnSize = 200
	}
	return &Parser{
		reg:        reg,
		splitter:   NewTxnSplitter(maxTxnSize),
		decoder:    NewRowDecoder(reg),
	}
}

// Parse converts raw entries into ordered change events. The version is
// resolved from the registry using the entry position, never from the live
// head version alone.
func (p *Parser) Parse(entries []source.LogEntry) ([]model.ChangeEvent, error) {
	events := make([]model.ChangeEvent, 0, len(entries))
	for _, entry := range entries {
		// Resolve the schema version that was active at the entry's own source
		// position, never the live head. A DDL applied later must not restructure
		// events written before it, and a rollback that replays old entries must
		// decode them with the layout that produced them.
		version, err := schema.BindVersion(p.reg, entry.Table, model.Position(entry.Seq))
		if err != nil || version == 0 {
			return nil, fmt.Errorf("resolve schema for %s at %d: %v", entry.Table, entry.Seq, err)
		}
		columns, err := p.decoder.Decode(entry, version)
		if err != nil {
			return nil, fmt.Errorf("decode %s seq=%d: %w", entry.Table, entry.Seq, err)
		}
		events = append(events, model.ChangeEvent{
			Seq:           entry.Seq,
			Table:         entry.Table,
			Op:            entry.Op,
			Columns:       columns,
			Key:           entry.Row["id"],
			TxnID:         entry.TxnID,
			TxnSeq:        entry.TxnSeq,
			SchemaVersion: version,
			SourcePos:     entry.Seq,
		})
	}
	return p.splitter.Sequence(events), nil
}

// Split re-chunks an existing event list into bounded delivery groups
// preserving transaction order. It satisfies the transport GroupSplitter
// contract used by the retry path.
func (p *Parser) Split(events []model.ChangeEvent) [][]model.ChangeEvent {
	return p.splitter.Split(events)
}
