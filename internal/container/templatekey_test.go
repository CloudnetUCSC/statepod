package container

import (
	"strings"
	"testing"

	"github.com/CloudnetUCSC/statepod/internal/workload"
)

// The unmodified built-ins keep the bare image ID (existing templates stay
// valid); a spec that keeps the name "redis" but changes the launch (AOF on)
// must get its own key, or it silently reuses plain Redis' baseline.
func TestTemplateKeyBuiltinVsVariant(t *testing.T) {
	const id = "sha256:abc"
	plain, _ := workload.Builtin("redis")
	pinned := *plain
	pinned.Image = "redis:7.4-alpine@sha256:abc"
	if got := (&Config{ImageID: id, Spec: &pinned}).TemplateKey(); got != id {
		t.Errorf("built-in redis (pinned image) key = %q, want bare %q", got, id)
	}
	node, _ := workload.Builtin("node")
	if got := (&Config{ImageID: id, Spec: node}).TemplateKey(); got != id {
		t.Errorf("built-in node key = %q, want bare %q", got, id)
	}

	aof := *plain
	aof.Args = []string{"redis-server", "--dir", "/data", "--appendonly", "yes", "--appendfsync", "everysec"}
	got := (&Config{ImageID: id, Spec: &aof}).TemplateKey()
	if got == id || !strings.HasPrefix(got, id+"+") {
		t.Errorf("redis-aof key = %q, want %q plus a spec fingerprint", got, id)
	}
}
