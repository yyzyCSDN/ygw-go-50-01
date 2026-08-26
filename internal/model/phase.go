package model

// SyncPhase describes the current synchronization stage.
type SyncPhase string

const (
	PhaseFull        SyncPhase = "full"
	PhaseIncremental SyncPhase = "incremental"
)

// PhaseCheckpoint records the handoff state between full and incremental sync.
type PhaseCheckpoint struct {
	Phase      SyncPhase
	SnapshotAt Position
	NextPos    Position
	Tables     []string
}
