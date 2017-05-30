package streammux

import (
	"context"
	"errors"
	"io"
)

type SparePool struct {
	ctx      context.Context
	shutdown context.CancelFunc
	spares   []io.ReadWriteCloser
	ch       chan io.ReadWriteCloser
}

func NewSparePool(rwcs []io.ReadWriteCloser) *SparePool {
	sp := &SparePool{
		spares: rwcs,
		ch:     make(chan io.ReadWriteCloser),
	}

	sp.ctx, sp.shutdown = context.WithCancel(context.Background())

	go sp.run()

	return sp
}

func (sp *SparePool) Shutdown() {
	sp.shutdown()
}

func (sp *SparePool) run() {
	defer close(sp.ch)

	for len(sp.spares) > 0 {
		spare := sp.spares[len(sp.spares)-1]
		sp.spares = sp.spares[:len(sp.spares)-1]

		select {
		case sp.ch <- spare:
		case <-sp.ctx.Done():
			return
		}
	}
}

type request struct{}

func (sp *SparePool) Get() (spare io.ReadWriteCloser, err error) {
	if sp == nil {
		return nil, errors.New("no spares")
	}

	spare = <-sp.ch
	if spare == nil {
		err = errors.New("no spares")
	}

	return
}
