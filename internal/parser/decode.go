package parser

import (
	"fmt"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/source"
)

// RowDecoder turns a raw source row into ordered column values using the
// schema version the event belongs to.
type RowDecoder struct {
	reg *schema.Registry
}

// NewRowDecoder returns a decoder bound to the schema registry.
func NewRowDecoder(reg *schema.Registry) *RowDecoder {
	return &RowDecoder{reg: reg}
}

// Decode maps a raw row into ordered column values. Missing columns decode as
// null; extra columns present in the row but absent from the schema are
// ignored so a forward-schema row never corrupts the ordered layout.
func (d *RowDecoder) Decode(entry source.LogEntry, version uint64) ([]model.ColumnValue, error) {
	tableSchema, err := d.reg.Schema(entry.Table, version)
	if err != nil {
		return nil, err
	}
	columns := make([]model.ColumnValue, 0, len(tableSchema.Columns))
	for _, def := range tableSchema.Columns {
		value, ok := entry.Row[def.Name]
		if !ok {
			columns = append(columns, model.ColumnValue{Name: def.Name, IsNull: true})
			continue
		}
		columns = append(columns, model.ColumnValue{Name: def.Name, Value: value})
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("row for %s has no decodable columns", entry.Table)
	}
	return columns, nil
}

// Encode builds a raw row from column values; used by tests and tooling that
// need a round trip through the decoder.
func (d *RowDecoder) Encode(columns []model.ColumnValue) map[string]string {
	row := make(map[string]string, len(columns))
	for _, col := range columns {
		if !col.IsNull {
			row[col.Name] = col.Value
		}
	}
	return row
}
