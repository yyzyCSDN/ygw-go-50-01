package model

// Position is a monotonic source position (log sequence number).
type Position uint64

// Checkpoint is the persisted synchronization progress.
type Checkpoint struct {
	Committed  Position
	Read       Position
	Phase      SyncPhase
	SnapshotAt Position
}
