package testutil

import (
	"io"
	"testing"
)

func TestBlockdev(t *testing.T) {
	blkdev := NewBlockDevice(10*len("foobar") + 4)

	data := []byte("foobar")

	for i := 0; i < 10; i++ {
		n, err := blkdev.Write(data)
		if err != nil {
			t.Fatalf("error: %v; wrote %d bytes", err, n)
		}

		if n != len(data) {
			t.Fatal("n != len(data) = %d", len(data))
		}

		if blkdev.pos != (i+1)*len(data) {
			t.Fatal("wrong position after write")
		}
	}

	// next write must fail with EOF, but proceed to write 4 bytes
	n, err := blkdev.Write([]byte("locoroco"))
	if err != io.EOF {
		t.Fatal(err)
	}

	if n != 4 {
		t.Fatal("write should have succeded in writing 4 bytes")
	}

	blkdev.Seek(0, io.SeekStart)

	for i := 0; i < 10; i++ {
		n, err := blkdev.Read(data)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v; read %d bytes", err, n)
		}

		if string(data) != "foobar" {
			t.Fatal("data mismatch")
		}
	}

	// next read must return EOF, but read 4 bytes
	n, err = blkdev.Read(data)
	if err != io.EOF {
		t.Fatal(err)
	}

	if n != 4 {
		t.Fatal("read should have succeded in reading 4 bytes")
	}

	if string(data[:n]) != "loco" {
		t.Fatalf("data mismatch: expected \"loco\", got %s", string(data))
	}
}
