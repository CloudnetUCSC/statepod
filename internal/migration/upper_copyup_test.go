package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CloudnetUCSC/statepod/internal/container"
)

// onlyCopiedUpDirs resolves the lowerdir relative to the bundle (the working
// directory of a migration), so the test runs inside a temporary bundle.
func TestOnlyCopiedUpDirs(t *testing.T) {
	bundle := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(bundle); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)
	for _, d := range []string{container.LowerDir + "/tmp", container.LowerDir + "/etc", container.UpperDir + "/data"} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatal(err)
		}
	}
	before := []string{"data"}

	// 1. the dump writes and removes a file in /tmp: overlayfs leaves a copied-up /tmp
	os.MkdirAll(filepath.Join(container.UpperDir, "tmp"), 0755)
	if p, ok := onlyCopiedUpDirs(container.UpperDir, before, []string{"data", "tmp"}); !ok {
		t.Fatalf("copied-up image dir counted as a change: %s", p)
	}
	// 2. a directory the image does not have is real state
	os.MkdirAll(filepath.Join(container.UpperDir, "newdir"), 0755)
	if _, ok := onlyCopiedUpDirs(container.UpperDir, before, []string{"data", "newdir"}); ok {
		t.Fatal("directory new to the image not counted as a change")
	}
	// 3. a new file is always a change
	os.WriteFile(filepath.Join(container.UpperDir, "tmp", "f"), []byte("x"), 0644)
	if _, ok := onlyCopiedUpDirs(container.UpperDir, before, []string{"data", "tmp", "tmp/f"}); ok {
		t.Fatal("new file not counted as a change")
	}
	// 4. a removed entry is always a change
	if _, ok := onlyCopiedUpDirs(container.UpperDir, []string{"data", "gone"}, []string{"data"}); ok {
		t.Fatal("removed entry not counted as a change")
	}
}
