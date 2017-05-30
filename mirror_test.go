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

func TestMirror(t *testing.T) {
	blkdevs := []io.ReadWriteCloser{
		testutil.NewBlockDevice(1 << 20),
		testutil.NewFaultyDevice(1<<20, 20),
	}

	m := streammux.NewMirror(blkdevs...)

	m.Open()

	data := make([]byte, 1<<20)

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
	m.Replace(1, testutil.NewBlockDevice(1<<20))

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
