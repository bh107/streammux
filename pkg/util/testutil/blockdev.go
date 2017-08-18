package testutil

import (
	"io"
	"syscall"
)

type BlockDevice struct {
	buf []byte
	eof int
	pos int
}

type FaultyDevice struct {
	*BlockDevice
	failAfter int
	nops      int
}

func NewFaultyDevice(size int, failAfter int) *FaultyDevice {
	return &FaultyDevice{
		BlockDevice: NewBlockDevice(size),
		failAfter:   failAfter,
	}
}

func (blk *FaultyDevice) Read(p []byte) (n int, err error) {
	if blk.nops >= blk.failAfter {
		return 0, syscall.EIO
	}

	blk.nops++

	return blk.BlockDevice.Read(p)
}

func (blk *FaultyDevice) Close() error {
	blk.nops = 0
	return blk.BlockDevice.Close()
}

func NewBlockDevice(size int) *BlockDevice {
	return &BlockDevice{
		buf: make([]byte, size),
		eof: -1,
	}
}

func (blk *BlockDevice) Close() error {
	blk.eof = blk.pos
	blk.pos = 0
	return nil
}

func (blk *BlockDevice) Read(p []byte) (n int, err error) {
	if blk.pos == blk.eof {
		return n, io.EOF
	}

	newpos := blk.pos + len(p)
	if (blk.pos + len(p)) > len(blk.buf) {
		newpos = len(blk.buf)
	}

	n = copy(p, blk.buf[blk.pos:newpos])
	blk.pos += n

	if n < len(p) {
		return n, io.EOF
	}

	return
}

func (blk *BlockDevice) Write(p []byte) (n int, err error) {
	newpos := blk.pos + len(p)

	if (blk.pos + len(p)) > len(blk.buf) {
		newpos = len(blk.buf)
	}

	n = copy(blk.buf[blk.pos:newpos], p)
	blk.pos += n

	if n < len(p) {
		return n, syscall.ENOSPC
	}

	return n, nil
}

func (blk *FaultyDevice) Write(p []byte) (n int, err error) {
	if blk.nops >= blk.failAfter {
		return 0, syscall.EIO
	}

	blk.nops++

	return blk.BlockDevice.Write(p)
}

func (blk *BlockDevice) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		blk.pos = int(offset)
	case io.SeekCurrent:
		blk.pos += int(offset)
	case io.SeekEnd:
		panic("not implemented")
	}

	if blk.pos < 0 {
		panic("seek to an offset before the start of the file")
	}

	return int64(blk.pos), nil
}

func (blk *FaultyDevice) Seek(offset int64, whence int) (int64, error) {
	if blk.nops >= blk.failAfter {
		return 0, syscall.EIO
	}

	blk.nops++

	return blk.BlockDevice.Seek(offset, whence)
}
