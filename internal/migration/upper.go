package migration

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/CloudnetUCSC/statepod/internal/container"
)

// upperChangedSince reports whether the overlay upper layer differs from what was
// last sent (the M4 catch-up resend decision, shared by synth-precopy and M5), and
// a short reason for the log. marker is a file written just before the send;
// rootAtSend is upperRootNames(upperDir) taken at the same moment.
//
// The root directory's own mtime is ignored (-mindepth 1): the runtime creates and
// removes a transient entry directly under the container root during the dump,
// which bumps only that mtime and made every catch-up migration resend the whole
// layer (measured 2026-10-05: 16 MB of files -> 33.6 MB shipped, +5-10 s of
// downtime). A real create/modify directly under the root is still caught by the
// entry's own mtime, and a deletion there by a difference in the root's entry
// names. Anything unreadable counts as changed (resend is always safe).
func upperChangedSince(ctx context.Context, upperDir, marker string, rootAtSend []string) (bool, string) {
	out, err := exec.CommandContext(ctx, "sudo", "find", upperDir,
		"-mindepth", "1", "-newer", marker, "-print", "-quit").Output()
	if err != nil {
		return true, "find failed: " + err.Error()
	}
	if p := bytes.TrimSpace(out); len(p) > 0 {
		return true, "newer path " + string(p)
	}
	now := upperRootNames(upperDir)
	if now == nil || rootAtSend == nil {
		return true, "root entries unreadable"
	}
	if !equalStrings(now, rootAtSend) {
		return true, "root entry set changed"
	}
	return false, ""
}

// upperRootNames lists the sorted entry names directly under dir, or nil if it
// cannot be read (callers treat nil as "unknown: assume changed").
func upperRootNames(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// upperPaths lists every entry of the upper layer (paths relative to it, sorted),
// or nil if it cannot be read (callers treat nil as "unknown: assume changed").
func upperPaths(ctx context.Context, upperDir string) []string {
	out, err := exec.CommandContext(ctx, "sudo", "find", upperDir, "-mindepth", "1", "-printf", "%P\n").Output()
	if err != nil {
		return nil
	}
	paths := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(paths) == 1 && paths[0] == "" {
		paths = paths[:0]
	}
	sort.Strings(paths)
	return paths
}

// upperTreeChangedSince is the filesystem pre-copy's round decision (fsprecopy.go):
// the layer changed since marker if a NON-directory entry is newer than marker
// (content, metadata or a new file), or if any entry appeared or disappeared
// (pathsAtSend = upperPaths at the marker). Directory mtimes are ignored: a
// directory's mtime moves whenever an entry is created and removed again, which the
// runtime does during every dump (a transient file in /tmp: 2026-10-27, and under
// the root: 2026-10-05), and that leaves nothing to send. Anything unreadable
// counts as changed (an extra round is always safe).
func upperTreeChangedSince(ctx context.Context, upperDir, marker string, pathsAtSend []string) (bool, string) {
	out, err := exec.CommandContext(ctx, "sudo", "find", upperDir, "-mindepth", "1", "!", "-type", "d",
		"-newer", marker, "-print", "-quit").Output()
	if err != nil {
		return true, "find failed: " + err.Error()
	}
	if p := bytes.TrimSpace(out); len(p) > 0 {
		return true, "newer path " + string(p)
	}
	now := upperPaths(ctx, upperDir)
	if now == nil || pathsAtSend == nil {
		return true, "entries unreadable"
	}
	if !equalStrings(now, pathsAtSend) {
		if p, ok := onlyCopiedUpDirs(upperDir, pathsAtSend, now); !ok {
			return true, "entry set changed: " + p
		}
	}
	return false, ""
}

// onlyCopiedUpDirs reports whether the entries that appeared since pathsAtSend
// are all directories that overlayfs merely copied up: the same directory exists
// in the image (lowerdir) and it is not opaque. Writing and removing a file in an
// image directory leaves exactly such an empty copy in the upper layer (the dump
// does it in /tmp, 2026-10-28); the merged view is unchanged, so there is nothing
// to send. Any removed entry, any non-directory, a directory new to the image or
// an opaque one (deleted and recreated: it hides the image's content) is a real
// change. Returns the first real change, or ok=true.
func onlyCopiedUpDirs(upperDir string, before, now []string) (string, bool) {
	old := make(map[string]bool, len(before))
	for _, p := range before {
		old[p] = true
	}
	cur := make(map[string]bool, len(now))
	for _, p := range now {
		cur[p] = true
		if old[p] {
			continue
		}
		st, err := os.Lstat(filepath.Join(upperDir, p))
		if err != nil || !st.IsDir() {
			return p, false
		}
		lst, err := os.Lstat(filepath.Join(container.LowerDir, p))
		if err != nil || !lst.IsDir() {
			return p, false
		}
		buf := make([]byte, 8)
		if n, err := syscall.Getxattr(filepath.Join(upperDir, p), "trusted.overlay.opaque", buf); err == nil && n > 0 {
			return p + " (opaque)", false
		}
	}
	for _, p := range before {
		if !cur[p] {
			return p + " (removed)", false
		}
	}
	return "", true
}
