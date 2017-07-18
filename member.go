package streammux

import (
	"io"
	"syscall"
)

type memberOptions struct {
	spares *SparePool
}

type MemberOption func(*memberOptions)

func WithSparePool(sp *SparePool) MemberOption {
	return func(o *memberOptions) {
		o.spares = sp
	}
}

type Member struct {
	rwc            io.ReadWriteCloser
	currentSegment int
	pos            int
	upto           int

	state State

	opts memberOptions

	segments []*segment
}

type segment struct {
	rwc  io.ReadWriteCloser
	upto int
}

// rwT represents an I/O request
type rwT struct {
	idx int
	p   []byte
	n   int
	err error
}

func NewMember(rwc io.ReadWriteCloser, opts ...MemberOption) *Member {
	m := &Member{
		rwc:      rwc,
		segments: make([]*segment, 0),
	}

	m.segments = append(m.segments, &segment{
		rwc:  rwc,
		upto: -1,
	})

	for _, opt := range opts {
		opt(&m.opts)
	}

	return m
}

func (m *Member) State() State {
	return m.state
}

func (m *Member) SetState(state State) {
	m.state = state
}

func (m *Member) Open() State {
	m.rwc = m.segments[0].rwc
	m.pos = 0
	m.upto = m.segments[0].upto
	m.currentSegment = 0

	if opener, ok := m.rwc.(Opener); ok {
		// call the underlying member and record the state
		m.state = opener.Open()
	}

	return m.state
}

func (m *Member) Close() error {
	return m.rwc.Close()
}

func (m *Member) write(idx int, p []byte, ch chan rwT) {
	if m.state != OK {
		ch <- rwT{idx, p, 0, syscall.EIO}
		return
	}

	for {
		n, err := m.rwc.Write(p)
		m.pos += n

		if err != nil && err != io.EOF {
			spare, err := m.opts.spares.Get()
			if err != nil {
				m.state = FAILED
			} else {
				m.segments[m.currentSegment].upto = m.pos
				m.currentSegment++

				// m.rwc holds up to m.pos
				m.segments = append(m.segments, &segment{
					rwc:  spare,
					upto: -1,
				})

				// close the old device
				m.rwc.Close()

				// update the current device
				m.rwc = spare

				// rest of write on new spare
				p = p[n:]
				continue
			}
		}

		ch <- rwT{idx, p, n, err}

		break
	}
}

func (m *Member) read(idx int, p []byte, ch chan rwT) {
	if m.state != OK {
		ch <- rwT{idx, p, 0, syscall.EIO}
		return
	}

	if m.upto != -1 && m.pos+len(p) > m.upto {
		m.currentSegment++
		m.rwc = m.segments[m.currentSegment].rwc
		if m.segments[m.currentSegment].upto != -1 {
			m.upto = m.upto + m.segments[m.currentSegment].upto
		} else {
			m.upto = -1
		}

		// open the device if needed
		if opener, ok := m.rwc.(Opener); ok {
			opener.Open()
		}

	}

	n, err := m.rwc.Read(p)

	if err != nil && err != io.EOF {
		m.state = FAILED
	}

	m.pos += n
	ch <- rwT{idx, p, n, err}

}
