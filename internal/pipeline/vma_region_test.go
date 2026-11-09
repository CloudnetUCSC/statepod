package pipeline

import "testing"

func TestVMARegion(t *testing.T) {
	cases := []struct {
		status uint32
		want   string
	}{
		{vmaAreaHeap | vmaAnonPrivate | 1, "heap"},
		{vmaAreaStack | vmaAnonPrivate | 1, "stack"},
		{vmaAnonShared | 1, "shared"},
		{vmaAreaSysvipc | vmaAnonShared | 1, "shared"},
		{vmaFileShared | 1, "shared"},
		{vmaFilePrivate | 1, "file-cow"},
		{vmaAnonPrivate | 1, "anon"},
		{vmaAreaVdso | 1, "other"},
	}
	for _, c := range cases {
		if got := (VMA{Status: c.status}).Region(); got != c.want {
			t.Errorf("status 0x%x: Region() = %q, want %q", c.status, got, c.want)
		}
	}
}
