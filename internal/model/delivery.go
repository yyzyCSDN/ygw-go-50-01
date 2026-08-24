package model

// DeliveryStatus tracks the per-event delivery state machine.
type DeliveryStatus int

const (
	StatusInFlight DeliveryStatus = iota
	StatusAcked
	StatusRetrying
)

func (s DeliveryStatus) String() string {
	switch s {
	case StatusAcked:
		return "acked"
	case StatusRetrying:
		return "retrying"
	default:
		return "in-flight"
	}
}

// DeliveryRecord is the observable state of one event in the pipeline.
type DeliveryRecord struct {
	EventSeq  uint64
	Table     string
	Status    DeliveryStatus
	Attempts  int
	LastError string
	TxnID     string
	TxnSeq    uint64
}
