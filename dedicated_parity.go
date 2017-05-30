package streammux

import (
	"io"
	"sync"
	"syscall"
)

type DedicatedParity struct {
	sync.Mutex

	stripe []*Member
	parity *Member

	state    State
	replaced chan int
}

func NewDedicatedParity(parity io.ReadWriteCloser, stripe []io.ReadWriteCloser, opts ...MemberOption) *DedicatedParity {
	dp := &DedicatedParity{
		stripe:   make([]*Member, len(stripe)),
		parity:   NewMember(parity, opts...),
		replaced: make(chan int),
	}

	for i, rwc := range stripe {
		dp.stripe[i] = NewMember(rwc, opts...)
	}

	return dp
}

func (dp *DedicatedParity) Open() {
	dp.Lock()

	for _, rwc := range append(dp.stripe, dp.parity) {
		rwc.Open()
	}

}

func (dp *DedicatedParity) Close() (err error) {
	defer dp.Unlock()

	for _, closer := range dp.stripe {
		err = closer.Close()
	}

	err = dp.parity.Close()

	return
}

func (dp *DedicatedParity) Read(p []byte) (n int, err error) {
	// THIS IS PRETTY HAIRY STUFF

	if dp.state == FAILED {
		return 0, syscall.EIO
	}

	ch := make(chan rwT)

	var active []struct{}

	stripe := split(p, len(dp.stripe))
	reconstructIdx := -1

	for i, reader := range append(dp.stripe, dp.parity) {
		if reader.State() != OK {
			reconstructIdx = i

			continue
		}

		go reader.read(i, make([]byte, len(p)/len(dp.stripe)), ch)

		active = append(active, struct{}{})
	}

	tmp := make(StripeBufferList, len(dp.stripe)+1)

	for range active {
		rc := <-ch

		n += rc.n
		err = rc.err

		if err != nil && err != io.EOF {
			if dp.state == DEGRADED {
				dp.state = FAILED
			} else {
				dp.state = DEGRADED
				reconstructIdx = rc.idx
				if rc.idx == len(dp.stripe) {
					dp.parity.SetState(FAILED)
				} else {
					dp.stripe[rc.idx].SetState(FAILED)
				}
			}

			continue
		}

		// save for reconstruction
		tmp[rc.idx] = rc.p[:rc.n]
	}

	// perform XOR only if one of the stripe members is FAILED
	if dp.state == DEGRADED && reconstructIdx != len(dp.stripe) {

		tmp2 := make(StripeBufferList, len(dp.stripe))

		var j int
		for i, buf := range tmp {
			if i == reconstructIdx {
				continue
			}

			tmp2[j] = buf
			copy(stripe[j], buf)
			j++
		}

		copy(stripe[reconstructIdx], tmp2.XOR())

		return
	}

	// if all is good, just copy the buffers into the stripe
	for i, buf := range tmp[:len(tmp)-1] {
		copy(stripe[i], buf)
	}

	return
}

func (dp *DedicatedParity) Write(p []byte) (n int, err error) {
	if dp.state == FAILED {
		return 0, syscall.EIO
	}

	ch := make(chan rwT)

	var active []struct{}

	stripe := split(p, len(dp.stripe))

	if dp.parity.State() == OK {
		go func() {
			r := stripe.XOR()

			dp.parity.write(len(dp.stripe), r, ch)
		}()

		active = append(active, struct{}{})
	}

	for i, writer := range dp.stripe {
		if writer.State() != OK {
			continue
		}

		go writer.write(i, stripe[i], ch)

		active = append(active, struct{}{})
	}

	var writeSucceeded bool
	var numGoodWrites int
	for range active {
		rc := <-ch

		n += rc.n

		if rc.err != nil && rc.err != io.EOF {
			if dp.state == DEGRADED {
				dp.state = FAILED
			} else {
				dp.state = DEGRADED
				if rc.idx == len(dp.stripe) {
					dp.parity.SetState(FAILED)
				} else {
					dp.stripe[rc.idx].SetState(FAILED)
				}
			}

			n -= rc.n

			if !writeSucceeded {
				err = rc.err
			}

			continue
		}

		numGoodWrites++

		if numGoodWrites == len(dp.stripe) {
			writeSucceeded = true
		}

		if writeSucceeded {
			err = rc.err
		}
	}

	if dp.state == OK {
		n -= len(p) / len(dp.stripe)
	}

	return
}
