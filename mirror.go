package streammux

import (
	"io"
	"sync"
	"syscall"
)

type Mirror struct {
	sync.Mutex
	seq int

	ios []*Member

	replaced chan int
	state    State

	spares *SparePool
}

func NewMirror(ios ...io.ReadWriteCloser) *Mirror {
	mirror := &Mirror{
		ios:      make([]*Member, len(ios)),
		replaced: make(chan int),
	}

	for i, streamer := range ios {
		mirror.ios[i] = NewMember(streamer)
	}

	go mirror.Sync()

	return mirror
}

func (m *Mirror) SetSparePool(sp *SparePool) {
	m.spares = sp
}

func (m *Mirror) Open() {
	m.Lock()

	for _, rwc := range m.ios {
		rwc.Open()
	}
}

func (m *Mirror) Close() (err error) {
	defer m.Unlock()

	for _, closer := range m.ios {
		err = closer.Close()
	}

	return
}

func (m *Mirror) Read(p []byte) (n int, err error) {
	ch := make(chan rwT)

	var active []struct{}

	for i, reader := range m.ios {
		if reader.State() != OK {
			continue
		}

		// allocate a new slice for the read
		go reader.read(i, make([]byte, len(p)), ch)

		active = append(active, struct{}{})
	}

	if len(active) == 0 {
		return 0, syscall.EIO
	}

	var readSucceeded bool

	for range active {
		rc := <-ch

		if rc.err != nil && rc.err != io.EOF {
			if len(active) > 1 {
				m.state = DEGRADED
				//log.Print("mirror DEGRADED")
			} else {
				m.state = FAILED
			}

			if !readSucceeded {
				n = rc.n
				err = rc.err
			}

			continue
		}

		if !readSucceeded {
			n = rc.n
			err = rc.err

			copy(p, rc.p[:n])
		}

		readSucceeded = true
	}

	return
}

func (m *Mirror) Write(p []byte) (n int, err error) {
	m.seq++
	ch := make(chan rwT)

	var active []struct{}

	for i, writer := range m.ios {
		if writer.State() != OK {
			continue
		}

		go writer.write(i, p, ch)

		active = append(active, struct{}{})
	}

	if len(active) == 0 {
		return 0, syscall.EIO
	}

	var writeSucceeded bool

	for range active {
		rc := <-ch

		//log.Printf("seq=%d, rc.n=%d, rc.err=%v", m.seq, rc.n, rc.err)

		if rc.err != nil && rc.err != io.EOF {
			if len(active) > 1 {
				m.state = DEGRADED
				//log.Print("mirror DEGRADED")
			} else {
				m.state = FAILED
			}

			if !writeSucceeded {
				n = rc.n
				err = rc.err
			}

			continue
		}

		if !writeSucceeded {
			n = rc.n
			err = rc.err
		}

		writeSucceeded = true
	}

	return
}

// Replace replaces the io.ReadWriteCloser at idx in mirror with rwc and
// signals the Sync function to begin synchronizing the mirror.
func (m *Mirror) Replace(idx int, rwc io.ReadWriteCloser) {
	m.Open()

	m.ios[idx] = NewMember(rwc)

	m.ios[idx].Open()
	/*
		if opener, ok := m.ios[idx].rwc.(Opener); ok {
			opener.Open()
		}
	*/

	m.replaced <- idx
}

func (m *Mirror) Sync() {
	for {
		// wait for a replaced drive
		idx := <-m.replaced

		// find a reader that is OK
		for _, reader := range m.ios {
			if reader.State() == OK {

				// rebuild the replaced writer by copying until EOF from the reader
				m.ios[idx].SetState(REBUILDING)

				written, err := io.Copy(m.ios[idx].rwc, reader.rwc)
				if err != nil {
					break
				}

				_ = written

				//log.Printf("finished rebuilding, copied %d bytes", written)

				m.ios[idx].SetState(OK)

				break
			}
		}

		m.Close()
	}
}
