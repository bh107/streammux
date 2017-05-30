package streammux

type State int

const (
	OK State = iota
	DEGRADED
	REBUILDING
	REPLACED
	FAILED
)

// An opener can be opened.
type Opener interface {
	Open()
}
