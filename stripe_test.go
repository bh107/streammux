package streammux_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"testing"

	"github.com/bh107/streammux"
	"github.com/bh107/streammux/pkg/util/testutil"
)

func TestStripe(t *testing.T) {
	blkdevs := []io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
	}

	s := streammux.NewStripe(blkdevs)

	s.Open()

	data := make([]byte, 1<<21)

	f, err := os.Open("/dev/urandom")
	if err != nil {
		t.Fatal(err)
	}

	n, err := f.Read(data)
	if err != nil || n != len(data) {
		t.Fatal(err)
	}

	origSha256Sum := sha256.Sum256(data)

	buf := bytes.NewBuffer(data)

	p := make([]byte, 1024)

	for {
		_, err := buf.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		n, err := s.Write(p)
		if err != nil || n != len(p) {
			t.Fatal(err)
		}
	}

	// close to reset position
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen
	s.Open()

	buf.Reset()

	for {
		_, err := s.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		_, err = buf.Write(p)
		if err != nil {
			t.Fatal(err)
		}
	}

	newSha256Sum := sha256.Sum256(buf.Bytes())

	if origSha256Sum != newSha256Sum {
		t.Fatal("origSha256Sum != newSha256Sum")
	}
}

func TestFailingStripeWithSparePool(t *testing.T) {
	blkdevs := []io.ReadWriteCloser{
		testutil.NewBlockDevice(16),
		testutil.NewFaultyDevice(16, 1),
	}

	sparePool := streammux.NewSparePool([]io.ReadWriteCloser{
		testutil.NewBlockDevice(16),
		testutil.NewBlockDevice(16),
	})

	s := streammux.NewStripe(blkdevs, streammux.WithSparePool(sparePool))

	data := []byte("locorocolocorocolocorocolocoroco")

	s.Open()

	/*
		data := make([]byte, 1<<21)

		f, err := os.Open("/dev/urandom")
		if err != nil {
			t.Fatal(err)
		}

		n, err := f.Read(data)
		if err != nil || n != len(data) {
			t.Fatal(err)
		}
	*/

	origSha256Sum := sha256.Sum256(data)

	buf := bytes.NewBuffer(data)

	p := make([]byte, 8)

	for {
		_, err := buf.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		n, err := s.Write(p)
		if err != nil || n != len(p) {
			t.Fatal(err)
		}
	}

	// close to reset position
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen
	s.Open()

	buf.Reset()

	for {
		_, err := s.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		_, err = buf.Write(p)
		if err != nil {
			t.Fatal(err)
		}
	}

	newSha256Sum := sha256.Sum256(buf.Bytes())

	if origSha256Sum != newSha256Sum {
		t.Fatal("origSha256Sum != newSha256Sum")
	}
}

func TestStripedMirror(t *testing.T) {
	blkdevs := []io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
		testutil.NewFaultyDevice(1<<20, 20),
	}

	mirrors := []io.ReadWriteCloser{
		streammux.NewMirror(blkdevs[:2]...),
		streammux.NewMirror(blkdevs[2:]...),
	}

	m := streammux.NewStripe(mirrors)

	m.Open()

	data := make([]byte, 1<<21)

	f, err := os.Open("/dev/urandom")
	if err != nil {
		t.Fatal(err)
	}

	n, err := f.Read(data)
	if err != nil || n != len(data) {
		t.Fatal(err)
	}

	origSha256Sum := sha256.Sum256(data)

	buf := bytes.NewBuffer(data)

	p := make([]byte, 1024)

	for {
		_, err := buf.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		n, err := m.Write(p)
		if err != nil || n != len(p) {
			t.Fatal(err)
		}
	}

	// close to reset position
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// replace failed drive (launched sync)
	mirrors[1].(*streammux.Mirror).Replace(1, testutil.NewBlockDevice(1<<20))

	// reopen
	m.Open()

	buf.Reset()

	for {
		_, err := m.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		_, err = buf.Write(p)
		if err != nil {
			t.Fatal(err)
		}
	}

	newSha256Sum := sha256.Sum256(buf.Bytes())

	if origSha256Sum != newSha256Sum {
		t.Fatal("origSha256Sum != newSha256Sum")
	}
}

func TestMirroredStripe(t *testing.T) {
	blkdevs := []io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
		testutil.NewFaultyDevice(1<<20, 20),
	}

	stripes := []*streammux.Stripe{
		streammux.NewStripe(blkdevs[:2]),
		streammux.NewStripe(blkdevs[2:]),
	}

	m := streammux.NewMirror(
		io.ReadWriteCloser(stripes[0]),
		io.ReadWriteCloser(stripes[1]),
	)

	m.Open()

	data := make([]byte, 1<<21)

	f, err := os.Open("/dev/urandom")
	if err != nil {
		t.Fatal(err)
	}

	n, err := f.Read(data)
	if err != nil || n != len(data) {
		t.Fatal(err)
	}

	origSha256Sum := sha256.Sum256(data)

	buf := bytes.NewBuffer(data)

	p := make([]byte, 1024)

	for {
		_, err := buf.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		n, err := m.Write(p)
		if err != nil || n != len(p) {
			t.Fatal(err, n)
		}
	}

	// close to reset position
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// replace failed drive (launched sync)
	m.Replace(1, streammux.NewStripe([]io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
	},
	))

	// reopen
	m.Open()

	buf.Reset()

	for {
		_, err := m.Read(p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		_, err = buf.Write(p)
		if err != nil {
			t.Fatal(err)
		}
	}

	newSha256Sum := sha256.Sum256(buf.Bytes())

	if origSha256Sum != newSha256Sum {
		t.Fatal("origSha256Sum != newSha256Sum")
	}
}
