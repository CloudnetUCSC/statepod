package migration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUpperRootNames(t *testing.T) {
	dir := t.TempDir()
	if got := upperRootNames(dir); got == nil || len(got) != 0 {
		t.Fatalf("empty dir: want empty non-nil slice, got %#v", got)
	}
	for _, n := range []string{"b", "a"} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0644); err != nil {
			t.Fatal(err)
		}
	}
	got := upperRootNames(dir)
	if !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("want sorted [a b], got %v", got)
	}
	if err := os.Remove(filepath.Join(dir, "a")); err != nil {
		t.Fatal(err)
	}
	if equalStrings(upperRootNames(dir), got) {
		t.Fatal("deleting a root entry must change the name set")
	}
	if upperRootNames(filepath.Join(dir, "missing")) != nil {
		t.Fatal("unreadable dir must return nil (unknown => assume changed)")
	}
}
