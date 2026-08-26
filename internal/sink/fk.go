package sink

import "cdcpipeline/internal/model"

// checkFK verifies that every foreign key parent referenced by the event
// already exists in the target. Writes are applied in dependency order by the
// mapper, so a violation here means the order contract was broken.
func (s *InMemorySink) checkFK(event model.ChangeEvent) error {
	if len(s.fk) == 0 || event.Op == model.OpDelete {
		return nil
	}
	parentOf := make(map[string]string)
	for _, edge := range s.fk {
		if edge.Child == event.Table {
			parentOf[edge.Parent] = edge.Parent
		}
	}
	for parent := range parentOf {
		if !s.parentHasKey(parent, event) {
			return ErrFKViolation
		}
	}
	return nil
}

func (s *InMemorySink) parentHasKey(parent string, event model.ChangeEvent) bool {
	parentRows := s.rows[parent]
	if len(parentRows) == 0 {
		return false
	}
	// The child row references its parent by the parent table's primary key
	// column named after the parent table plus "_id".
	refColumn := parent + "_id"
	refValue, ok := event.Column(refColumn)
	if !ok {
		return true
	}
	_, exists := parentRows[refValue]
	return exists
}
