package log

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello, it is test")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendAndRead(t *testing.T) {
	file, err := os.CreateTemp("", "test_file_append_and_read")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(file.Name()); err != nil {
			log.Fatal("failed to remote tmp file", err)
		}
	}()

	st, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, st)
	testRead(t, st)
	testReadAt(t, st)

	st, err = newStore(file)
	require.NoError(t, err)
	testRead(t, st)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	file, err := os.CreateTemp("", "test_store_close")
	require.NoError(t, err)
	defer func() {
		if err := os.Remove(file.Name()); err != nil {
			log.Fatal("failed to remove tmp file", err)
		}
	}()

	st, err := newStore(file)
	require.NoError(t, err)

	_, _, err = st.Append(write)
	require.NoError(t, err)

	_, beforeSize, err := openFile(file.Name())
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(file.Name())
	require.NoError(t, err)

	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}

	return file, stat.Size(), nil
}
