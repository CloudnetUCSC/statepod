package workload

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuiltinsValidate(t *testing.T) {
	for _, n := range []string{"redis", "node"} {
		s, ok := Builtin(n)
		if !ok {
			t.Fatalf("builtin %q missing", n)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("builtin %q invalid: %v", n, err)
		}
	}
	if _, ok := Builtin("nope"); ok {
		t.Error("unknown builtin should not resolve")
	}
}

// The built-ins must reproduce the pre-Spec behaviour exactly, or every
// existing template/baseline silently stops matching.
func TestBuiltinsMatchLegacyBehaviour(t *testing.T) {
	r, _ := Builtin("redis")
	if r.Port != 6379 || r.DataDir != "data" || !r.Stateful || r.Ready.Kind != ReadyRESP {
		t.Errorf("redis spec drifted: %+v", r)
	}
	if got := r.Args; len(got) != 3 || got[0] != "redis-server" || got[1] != "--dir" || got[2] != "/data" {
		t.Errorf("redis args drifted: %v", got)
	}
	n, _ := Builtin("node")
	if n.Port != 3000 || n.Stateful || n.Ready.Kind != ReadyTCP || len(n.Provision) != 1 || n.Provision[0].Path != "app/server.js" {
		t.Errorf("node spec drifted: %+v", n)
	}
}

func TestLoadFileAndValidation(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "pg.json")
	os.WriteFile(good, []byte(`{"name":"postgres","port":5432,"args":["postgres"],"ready":{"kind":"exec","cmd":["pg_isready"]},"stateful":true,"data_dir":"var/lib/postgresql/data"}`), 0644)
	s, err := Load(good)
	if err != nil || s.Name != "postgres" || s.Ready.Kind != ReadyExec {
		t.Fatalf("Load good spec: %v %+v", err, s)
	}

	for name, body := range map[string]string{
		"noport":  `{"name":"x","args":["a"],"ready":{"kind":"tcp"}}`,
		"noargs":  `{"name":"x","port":1,"ready":{"kind":"tcp"}}`,
		"badkind": `{"name":"x","port":1,"args":["a"],"ready":{"kind":"magic"}}`,
		"nohttp":  `{"name":"x","port":1,"args":["a"],"ready":{"kind":"http"}}`,
		"escape":  `{"name":"x","port":1,"args":["a"],"ready":{"kind":"tcp"},"provision":[{"path":"../etc/x","content":"y"}]}`,
	} {
		p := filepath.Join(dir, name+".json")
		os.WriteFile(p, []byte(body), 0644)
		if _, err := Load(p); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	if _, err := Load(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("missing file should error")
	}
}
