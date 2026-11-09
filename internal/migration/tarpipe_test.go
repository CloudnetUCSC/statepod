package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The residual request can fail before reading its body (destination unreachable):
// closing the reader must end the tar, or the source waits forever while paused.
func TestTarPipeReaderStopsWhenReaderCloses(t *testing.T) {
	dir := t.TempDir()
	// larger than the kernel's pipe buffer, so tar is blocked writing
	if err := os.WriteFile(filepath.Join(dir, "big"), make([]byte, 4<<20), 0644); err != nil {
		t.Fatal(err)
	}
	r, errCh, err := tarPipeReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	r.CloseWithError(errors.New("destination unreachable"))
	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("tar stream still blocked after its reader closed")
	}
}
