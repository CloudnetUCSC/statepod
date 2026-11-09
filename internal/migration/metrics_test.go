package migration

import (
	"testing"
	"time"
)

func TestMarkRecordsAndOverwrites(t *testing.T) {
	m := &MigrationMetrics{}
	t1 := m.Mark("src_a")
	if got := m.Phases["src_a"]; !got.Equal(t1) {
		t.Fatalf("Phases[src_a] = %v, want %v", got, t1)
	}
	time.Sleep(2 * time.Millisecond)
	t2 := m.Mark("src_a")
	if !t2.After(t1) || !m.Phases["src_a"].Equal(t2) {
		t.Fatalf("re-mark should overwrite with the later time")
	}
}

func TestMergePhasesSourceWins(t *testing.T) {
	m := &MigrationMetrics{}
	src := m.Mark("src_kill")
	dst := time.Now().Add(-time.Hour)
	m.MergePhases(map[string]time.Time{"dst_restore_end": dst, "src_kill": dst})
	if !m.Phases["src_kill"].Equal(src) {
		t.Fatalf("existing key must not be overwritten by merge")
	}
	if !m.Phases["dst_restore_end"].Equal(dst) {
		t.Fatalf("new key from other host should be merged")
	}
	var empty MigrationMetrics
	empty.MergePhases(nil)
	if empty.Phases != nil {
		t.Fatalf("merging nothing must not allocate")
	}
}
