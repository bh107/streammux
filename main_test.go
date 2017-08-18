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

func TestStreammux(t *testing.T) {
	blkdevs := []io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 19),
	}

	sparePool := streammux.NewSparePool([]io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewBlockDevice(1 << 20),
	})

	s := streammux.NewStripe(blkdevs, streammux.WithSparePool(sparePool))

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
