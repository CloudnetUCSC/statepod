// Package workload defines WHAT statepod migrates: one declarative Spec per
// workload (image, service port, how to start it, how to know it is ready,
// what filesystem state it keeps), replacing the Redis/Node booleans that were
// hard-coded through container creation, readiness and the experiment runner
// . Adding a workload (PostgreSQL, a JVM service, the synthetic knob
// workload) is a new Spec — a built-in below or a JSON file passed to
// `statepod create --workload` — not a new code path.
//
// The Spec is embedded in statepod.json at create time, so a destination that
// only receives statepod.json knows how to probe the workload without any
// registry of its own.
package workload

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Readiness kinds. The readiness point is where template synthesis (M1) and
// source start both anchor (§3.2), so it must mean "the workload is
// idle and fully initialised", not merely "the port is open".
const (
	ReadyTCP  = "tcp"  // accepting connections on the service port
	ReadyRESP = "resp" // RESP `PING` answered with +PONG (Redis; listening alone is not enough, it accepts while -LOADING)
	ReadyHTTP = "http" // GET Path answers with a 2xx/3xx status
	ReadyExec = "exec" // `runc exec <id> Cmd...` exits 0 (e.g. pg_isready)
)

// Readiness describes how to detect that the workload is ready.
type Readiness struct {
	Kind string   `json:"kind"`
	Path string   `json:"path,omitempty"` // ReadyHTTP
	Cmd  []string `json:"cmd,omitempty"`  // ReadyExec, run inside the container
}

// File is provisioned into the image's lower layer before the first start
// (identical on source and destination, never migrated).
type File struct {
	Path    string `json:"path"` // relative to the rootfs, e.g. "app/server.js"
	Mode    uint32 `json:"mode,omitempty"`
	Content string `json:"content"`
}

// Spec is the full description of a workload.
type Spec struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Image       string   `json:"image,omitempty"` // default image reference (experiments pin a digest in hosts.yaml)
	Port        int      `json:"port"`            // service port inside the container
	Args        []string `json:"args"`            // process.args, before the deterministic-launch wrapper
	Env         []string `json:"env,omitempty"`   // appended to process.env
	Cwd         string   `json:"cwd,omitempty"`   // process.cwd (default "/")
	User        string   `json:"user,omitempty"`  // "uid:gid" for process.user (default: image's, i.e. root)
	// Capabilities are ADDED to runc's default set (AUDIT_WRITE, KILL,
	// NET_BIND_SERVICE) in every capability set, e.g. CAP_CHOWN, CAP_SETUID,
	// CAP_SETGID, CAP_FOWNER, CAP_DAC_OVERRIDE for an image whose entrypoint
	// chowns its data directory and drops privileges (PostgreSQL).
	Capabilities []string  `json:"capabilities,omitempty"`
	Ready        Readiness `json:"ready"`
	Provision    []File    `json:"provision,omitempty"`
	// DataDir is a rootfs-relative directory holding state the workload
	// persists (Redis /data). The restart baseline syncs it (volume sync);
	// M4 moves it as part of the overlay upper layer. Empty = no persistent
	// directory.
	DataDir string `json:"data_dir,omitempty"`
	// Stateful is the dispatcher's classification (burst: stateful -> live
	// migration, stateless -> restart).
	Stateful bool `json:"stateful"`
}

// nodeServerJS is the bundled Node.js server (kept byte-identical to the
// pre-Spec createNodeServer so existing templates/baselines stay valid).
const nodeServerJS = `var http = require('http');

var server = http.createServer(function (req, res) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Hello from Node.js 4 server running inside runc container!\n');
});

server.listen(3000, function () {
    console.log('Node.js 4 server is running on port ' + 3000);
});`

// Builtin returns the named built-in spec. "redis" and "node" reproduce the
// behaviour statepod had before Specs existed, byte for byte.
func Builtin(name string) (*Spec, bool) {
	switch name {
	case "redis":
		return &Spec{
			Name:        "redis",
			Description: "Redis (stateful in-memory key-value store)",
			Image:       "redis:7.4-alpine",
			Port:        6379,
			Args:        []string{"redis-server", "--dir", "/data"},
			Ready:       Readiness{Kind: ReadyRESP},
			DataDir:     "data",
			Stateful:    true,
		}, true
	case "node":
		return &Spec{
			Name:        "node",
			Description: "Bundled Node.js HTTP server (stateless)",
			Image:       "node:4-alpine",
			Port:        3000,
			Args:        []string{"node", "/app/server.js"},
			Ready:       Readiness{Kind: ReadyTCP},
			Provision:   []File{{Path: "app/server.js", Mode: 0644, Content: nodeServerJS}},
			Stateful:    false,
		}, true
	}
	return nil, false
}

// Load resolves a --workload argument: a built-in name, or the path of a JSON
// spec file.
func Load(nameOrPath string) (*Spec, error) {
	if s, ok := Builtin(nameOrPath); ok {
		return s, nil
	}
	data, err := os.ReadFile(nameOrPath)
	if err != nil {
		return nil, fmt.Errorf("workload %q is neither a built-in (redis, node) nor a readable spec file: %w", nameOrPath, err)
	}
	var s Spec
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parsing workload spec %s: %w", nameOrPath, err)
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("workload spec %s: %w", nameOrPath, err)
	}
	return &s, nil
}

// Validate rejects specs that cannot be started or probed.
func (s *Spec) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if s.Port <= 0 || s.Port > 65535 {
		return fmt.Errorf("port %d out of range", s.Port)
	}
	if len(s.Args) == 0 {
		return fmt.Errorf("args (the workload command) is required")
	}
	switch s.Ready.Kind {
	case ReadyTCP, ReadyRESP:
	case ReadyHTTP:
		if s.Ready.Path == "" {
			return fmt.Errorf("ready.kind=http needs ready.path")
		}
	case ReadyExec:
		if len(s.Ready.Cmd) == 0 {
			return fmt.Errorf("ready.kind=exec needs ready.cmd")
		}
	default:
		return fmt.Errorf("unknown ready.kind %q (want tcp, resp, http or exec)", s.Ready.Kind)
	}
	for _, c := range s.Capabilities {
		if !strings.HasPrefix(c, "CAP_") || strings.ToUpper(c) != c {
			return fmt.Errorf("capability %q must look like CAP_CHOWN", c)
		}
	}
	for _, f := range s.Provision {
		if f.Path == "" || strings.HasPrefix(f.Path, "/") || strings.Contains(f.Path, "..") {
			return fmt.Errorf("provision path %q must be relative and stay inside the rootfs", f.Path)
		}
	}
	return nil
}
