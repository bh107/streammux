package streammux

import (
	"io"
	"strconv"
	"sync"
	"syscall"
)

type StripeBuffer []byte

type StripeBufferList []StripeBuffer

func split(p StripeBuffer, stripeWidth int) StripeBufferList {
	if len(p)%wordSize != 0 {
		panic("buffer must be a multiple of the architecture wordSize (" + strconv.Itoa(wordSize) + ")")
	}

	stripeSize := len(p) / stripeWidth
	lst := make(StripeBufferList, stripeWidth)
	for i := 0; i < stripeWidth; i++ {
		lst[i] = p[i*stripeSize : i*stripeSize+stripeSize]
	}

	return lst
}

func (src StripeBufferList) XOR() StripeBuffer {
	if len(src) < 2 {
		panic("cannot xor less than two byte arrays")
	}

	dst := make(StripeBuffer, len(src[0]))

	a := src[0]

	for i := 1; i < len(src); i++ {
		xorWords(dst, a, src[i])
		a = dst
	}

	return dst
}

type Stripe struct {
	seq int
	sync.Mutex

	ios []*Member

	state State
}

func NewStripe(rwcs []io.ReadWriteCloser, opts ...MemberOption) *Stripe {
	stripe := &Stripe{
		ios: make([]*Member, len(rwcs)),
	}

	for i, rwc := range rwcs {
		stripe.ios[i] = NewMember(rwc, opts...)
	}

	return stripe
}

func (s *Stripe) Open() {
	s.Lock()

	for _, rwc := range s.ios {
		rwc.Open()
	}
}

func (s *Stripe) Close() (err error) {
	defer s.Unlock()

	for _, closer := range s.ios {
		err = closer.Close()
	}

	return
}

func (s *Stripe) Read(p []byte) (n int, err error) {
	if s.state != OK {
		return 0, syscall.EIO
	}

	ch := make(chan rwT)

	stripe := split(p, len(s.ios))

	for i, reader := range s.ios {
		go reader.read(i, stripe[i], ch)
	}

	for range s.ios {
		rc := <-ch

		n += rc.n
		err = rc.err

		if err != nil && err != io.EOF {
			s.state = FAILED
		}
	}

	return
}

func (s *Stripe) Write(p []byte) (n int, err error) {
	s.seq++
	if s.state != OK {
		return 0, syscall.EIO
	}

	ch := make(chan rwT)

	stripe := split(p, len(s.ios))

	for i, writer := range s.ios {
		go writer.write(i, stripe[i], ch)
	}

	for range s.ios {
		rc := <-ch

		//log.Printf("[s] seq=%d, rc.n=%d, rc.err=%v", s.seq, rc.n, rc.err)
		if rc.err != nil && rc.err != io.EOF {
			s.state = FAILED

			err = rc.err
			continue
		}

		if rc.err != nil {
			err = rc.err
		}

		n += rc.n
	}

	//log.Printf("[s return] seq=%d, n=%d, err=%v", s.seq, n, err)
	return
}
