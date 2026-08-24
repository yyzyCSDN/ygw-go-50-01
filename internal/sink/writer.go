package sink

import (
	"context"

	"cdcpipeline/internal/model"
)

// applyWithContext writes a row and records the application order. The context
// is consulted before and after the row is mutated so a timed-out write never
// looks successful.
func (s *InMemorySink) applyWithContext(ctx context.Context, event model.ChangeEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkFK(event); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[event.Table]; !ok {
		s.rows[event.Table] = make(map[string]map[string]string)
	}
	key := event.Key
	if key == "" {
		key = event.TableKey()
	}
	switch event.Op {
	case model.OpDelete:
		delete(s.rows[event.Table], key)
	default:
		row := make(map[string]string, len(event.Columns))
		for _, col := range event.Columns {
			if !col.IsNull {
				row[col.Name] = col.Value
			}
		}
		if _, ok := row["id"]; !ok {
			row["id"] = key
		}
		s.rows[event.Table][key] = row
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.order = append(s.order, event)
	return nil
}
