package streammux

type State int

const (
	// OK indicates that the member is working as intended.
	OK State = iota

	// DEGRADED indicates that the member is degraded, but functional.
	DEGRADED

	// REBUILDING indicates that the member is currently rebuilding.
	REBUILDING

	// REPLACED indicates that the member has been replaced and is awaiting rebuild.
	REPLACED

	// FAILED indicates that the member as failed.
	FAILED
)

// Opener is implemented by types that can be opened, returning a state.
type Opener interface {
	Open() State
}

// Behavior is implemented by redundancy behaviors.
type Behavior interface {
	Health() State
}
