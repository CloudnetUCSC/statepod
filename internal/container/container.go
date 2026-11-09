package container

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/workload"
)

type Config struct {
	ContainerID  string `json:"container_id"`
	DockerSource string `json:"docker_source"`
	ImageID      string `json:"image_id,omitempty"`
	BundlePath   string `json:"bundle_path"`
	RootFS       string `json:"rootfs"`
	CreatedAt    string `json:"created_at"`
	CurrentState string `json:"current_state"`
	CurrentPID   int    `json:"current_pid"`
	IsRedis      bool   `json:"is_redis,omitempty"` // legacy: true iff Workload == "redis"; kept so older statepod.json files and sync.go's SAVE path keep working
	// Workload/Spec: which workload this bundle runs and its full
	// declarative description. Spec travels inside statepod.json, so a
	// destination that only receives that file can probe the workload
	// without a registry of its own. Both empty for pre-Spec bundles and for
	// plain (no --workload/--redis/--with-node-server) containers.
	Workload      string         `json:"workload,omitempty"`
	Spec          *workload.Spec `json:"spec,omitempty"`
	Stateful      bool           `json:"stateful,omitempty"`
	Deterministic bool           `json:"deterministic,omitempty"`
	VolumeSync    bool           `json:"volume_sync,omitempty"`
	VolumePath    string         `json:"volume_path,omitempty"`
	Subnet        string         `json:"subnet,omitempty"`
	Gateway       string         `json:"gateway,omitempty"`
	ContainerIP   string         `json:"container_ip,omitempty"`
	HostPort      int            `json:"host_port,omitempty"`
	RsyncPort     int            `json:"rsync_port,omitempty"`
}

// RedisHostPort returns the host port DNAT-forwarded to the container's
// Redis port (6379). Defaults to 6380 for configs written before the
// host_port field existed.
func (c *Config) RedisHostPort() int {
	if c.HostPort != 0 {
		return c.HostPort
	}
	return 6380
}

// ServicePort returns the in-container port the workload listens on: the
// Spec's port when there is one, else (pre-Spec bundles) Redis on 6379 and the
// bundled Node server on 3000.
func (c *Config) ServicePort() int {
	if c.Spec != nil && c.Spec.Port > 0 {
		return c.Spec.Port
	}
	if c.IsRedis {
		return 6379
	}
	return 3000
}

// TemplateKey names this bundle's baseline in the destination's template store.
// For the built-in workloads (redis, node) it is the image ID alone, exactly as
// before. For any other workload it is the image ID plus a fingerprint of the
// Spec: a parameterised workload (e.g. the synthetic one at 16 MB vs 256 MB
// resident) cold-starts to a DIFFERENT dump per parameter set, so sharing one
// baseline per image would silently reuse the wrong one and wreck the hit rate.
func (c *Config) TemplateKey() string {
	// Only the unmodified built-in keeps the bare image ID. A spec that keeps
	// the name (so the engine's per-workload handling applies) but changes
	// the launch, e.g. Redis with AOF on, cold-starts to a different dump and
	// needs its own baseline: matching on the name alone made Redis-AOF share
	// plain Redis' template.
	if c.Spec == nil || isBuiltinSpec(c.Spec) {
		return c.ImageID
	}
	data, err := json.Marshal(c.Spec)
	if err != nil {
		return c.ImageID
	}
	sum := sha256.Sum256(data)
	return c.ImageID + "+" + hex.EncodeToString(sum[:])[:8]
}

// isBuiltinSpec reports whether s is exactly a built-in workload spec. The
// image reference is ignored: experiments pin a digest the built-in does not.
func isBuiltinSpec(s *workload.Spec) bool {
	b, ok := workload.Builtin(s.Name)
	if !ok {
		return false
	}
	x, y := *s, *b
	x.Image, y.Image = "", ""
	xj, err1 := json.Marshal(x)
	yj, err2 := json.Marshal(y)
	return err1 == nil && err2 == nil && string(xj) == string(yj)
}

// RsyncDaemonPort returns the port the receiver's rsync daemon listens on
// (--mode precopy-rsync / sync-rsync transport addendum). Defaults
// to 8730 for configs written before the field existed.
func (c *Config) RsyncDaemonPort() int {
	if c.RsyncPort != 0 {
		return c.RsyncPort
	}
	return 8730
}

type RuncState struct {
	ID     string `json:"id"`
	Pid    int    `json:"pid"`
	Status string `json:"status"`
}

func CreateConfig(containerID string, image string, imageID string, spec *workload.Spec, stateful bool, deterministic bool, subnet, gateway, ip string, hostPort int) error {
	var volumePath, workloadName string
	if spec != nil {
		volumePath = spec.DataDir
		workloadName = spec.Name
	}

	cfg := Config{
		ContainerID:   containerID,
		DockerSource:  image,
		ImageID:       imageID,
		BundlePath:    ".",
		RootFS:        "./rootfs",
		CreatedAt:     time.Now().Format(time.RFC3339),
		CurrentState:  "stopped",
		CurrentPID:    0,
		IsRedis:       workloadName == "redis",
		Workload:      workloadName,
		Spec:          spec,
		Stateful:      stateful,
		Deterministic: deterministic,
		VolumePath:    volumePath,
		Subnet:        subnet,
		Gateway:       gateway,
		ContainerIP:   ip,
		HostPort:      hostPort,
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile("statepod.json", data, 0644)
}

func ReadConfig() (*Config, error) {
	data, err := os.ReadFile("statepod.json")
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func UpdateConfig(cfg *Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("statepod.json", data, 0644)
}

func GetRuncState(ctx context.Context, id string) (*RuncState, error) {
	out, err := exec.CommandContext(ctx, "sudo", "runc", "state", id).Output()
	if err != nil {
		return nil, err
	}

	var state RuncState

	if err := json.Unmarshal(out, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func writeReadme(image string) error {
	content := "# StatePod Workspace\n\n"
	content += "This directory was initialized with statepod.\n\n"
	content += "Source Image: `" + image + "`\n\n"
	content += "## Commands\n\n"
	content += "Run container:\n\n"
	content += "```\nstatepod start\n```\n\n"
	content += "Stop container:\n\n"
	content += "```\nstatepod stop\n```\n\n"
	content += "You can modify `config.json` to customize runtime behavior.\n"

	return os.WriteFile("README.md", []byte(content), 0644)
}

// provisionFiles writes a workload's Provision files into the (lower) rootfs
// before first start. Paths are validated relative by workload.Spec.Validate;
// re-checked here because a Spec can also arrive embedded in statepod.json.
func provisionFiles(rootfsBase string, files []workload.File) error {
	for _, f := range files {
		if f.Path == "" || filepath.IsAbs(f.Path) || strings.Contains(f.Path, "..") {
			return fmt.Errorf("unsafe provision path %q", f.Path)
		}
		dst := filepath.Join(rootfsBase, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		mode := os.FileMode(f.Mode)
		if mode == 0 {
			mode = 0644
		}
		if err := os.WriteFile(dst, []byte(f.Content), mode); err != nil {
			return err
		}
	}
	return nil
}

// patchOCIConfig points runc's generated config.json at the workload: its
// command (behind the ASLR-off wrapper when deterministic), cwd, extra env and
// user, with a writable rootfs. For the redis and node built-ins this
// produces exactly the config the former patchRedisConfig/patchConfig did.
func patchOCIConfig(spec *workload.Spec, deterministic bool) error {
	data, err := os.ReadFile("config.json")
	if err != nil {
		return err
	}

	var config map[string]interface{}
	if err = json.Unmarshal(data, &config); err != nil {
		return err
	}

	process := config["process"].(map[string]interface{})
	root := config["root"].(map[string]interface{})

	process["terminal"] = false
	process["args"] = wrapArgs(spec.Args, deterministic)
	cwd := spec.Cwd
	if cwd == "" {
		cwd = "/"
	}
	process["cwd"] = cwd
	if len(spec.Env) > 0 {
		// A Spec variable REPLACES an existing one of the same name (the image's
		// own ENV is lost by `docker export`, so a Spec typically re-states PATH
		// etc.); duplicates would leave the first one winning in getenv().
		env, _ := process["env"].([]interface{})
		for _, kv := range spec.Env {
			name := kv
			if i := strings.Index(kv, "="); i >= 0 {
				name = kv[:i]
			}
			kept := env[:0:0]
			for _, e := range env {
				if es, ok := e.(string); ok && strings.HasPrefix(es, name+"=") {
					continue
				}
				kept = append(kept, e)
			}
			env = append(kept, kv)
		}
		process["env"] = env
	}
	if len(spec.Capabilities) > 0 {
		caps, _ := process["capabilities"].(map[string]interface{})
		if caps == nil {
			caps = map[string]interface{}{}
			process["capabilities"] = caps
		}
		for _, set := range []string{"bounding", "effective", "permitted", "inheritable", "ambient"} {
			cur, _ := caps[set].([]interface{})
			have := map[string]bool{}
			for _, c := range cur {
				if cs, ok := c.(string); ok {
					have[cs] = true
				}
			}
			for _, c := range spec.Capabilities {
				if !have[c] {
					cur = append(cur, c)
				}
			}
			caps[set] = cur
		}
	}
	if spec.User != "" {
		var uid, gid int
		if _, err := fmt.Sscanf(spec.User, "%d:%d", &uid, &gid); err != nil {
			return fmt.Errorf("workload user %q must be uid:gid: %w", spec.User, err)
		}
		process["user"] = map[string]interface{}{"uid": uid, "gid": gid}
	}

	root["path"] = "rootfs"
	root["readonly"] = false

	newData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile("config.json", newData, 0644)
}

// nrandRootfsPath is where the deterministic-launch wrapper lands inside the
// container rootfs. It must be identical on source and destination bundles so
// template synthesis  launches the workload the same way.
const nrandRootfsPath = "statepod-nrand"

// wrapArgs prepends the ASLR-off wrapper to the workload entrypoint when the
// bundle was created with deterministic launch .
func wrapArgs(args []string, deterministic bool) []string {
	if !deterministic {
		return args
	}
	return append([]string{"/" + nrandRootfsPath}, args...)
}

// installNrand copies the statically linked nrand helper (built alongside
// statepod, see Makefile) from the statepod bin dir into the bundle rootfs.
func installNrand(rootfsBase string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	src := filepath.Join(filepath.Dir(exe), "nrand")
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("deterministic launch requires the nrand helper at %s (run `make build`): %w", src, err)
	}
	return os.WriteFile(filepath.Join(rootfsBase, nrandRootfsPath), data, 0755)
}

// Overlay dir names for the stateful-path rootfs ,
// relative to the bundle root. lowerdir holds the prefetched, read-only image
// content (provisioning: extracted image layers, nrand wrapper, node server
// script — identical on source and destination, never migrated). upperdir
// holds runtime writes and is the only filesystem data the M4 migration step
// streams. "rootfs" is the overlay mount (merged) that runc/CRIU see —
// unchanged from the flat-rootfs path's config.json root.path, so the rest of
// the codebase (sync.go, receiver.go, config.json) needs no changes to keep
// working against either layout.
const (
	overlayLowerDir = "lower"
	overlayUpperDir = "upper"
	overlayWorkDir  = "work"
)

// UpperDir is the overlay upperdir path relative to the bundle root — the
// only filesystem data the M4 migration step streams.
const UpperDir = overlayUpperDir

// LowerDir is the overlay lowerdir (the image content) relative to the bundle root.
const LowerDir = overlayLowerDir

// MountOverlay mounts the stateful-path overlay rootfs at "rootfs" from the
// bundle's lower/upper/work dirs. Idempotent: a no-op if already mounted.
func MountOverlay() error {
	mounted, err := isRootfsMounted()
	if err != nil {
		return err
	}
	if mounted {
		return nil
	}
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", overlayLowerDir, overlayUpperDir, overlayWorkDir)
	if out, err := exec.Command("sudo", "mount", "-t", "overlay", "overlay", "-o", opts, "rootfs").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to mount overlay rootfs: %w (%s)", err, out)
	}
	return nil
}

// UnmountOverlay tears down the overlay mount at "rootfs", leaving
// lower/upper/work on disk intact. Safe to call when not mounted (e.g. a
// flat-rootfs bundle, or already torn down). Callers that discard a bundle
// directory (template.go's throwaway instances) must call this before
// removing it — deleting a directory a live mount still points at leaves a
// stale mount entry on the host.
func UnmountOverlay() error {
	mounted, err := isRootfsMounted()
	if err != nil {
		return err
	}
	if !mounted {
		return nil
	}
	if out, err := exec.Command("sudo", "umount", "rootfs").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to unmount overlay rootfs: %w (%s)", err, out)
	}
	return nil
}

func isRootfsMounted() (bool, error) {
	err := exec.Command("mountpoint", "-q", "rootfs").Run()
	if err == nil {
		return true, nil
	}
	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}
	return false, fmt.Errorf("mountpoint check failed for rootfs: %w", err)
}

func CreateContainer(ctx context.Context, image string, wl *workload.Spec, stateful bool, deterministic bool, subnet, gateway, ip string, hostPort int) error {
	fmt.Println("Creating container from image:", image)
	if wl != nil {
		if err := wl.Validate(); err != nil {
			return fmt.Errorf("invalid workload spec: %w", err)
		}
		fmt.Println("Workload:", wl.Name)
	}

	// docker create
	out, err := exec.CommandContext(ctx, "docker", "create", image).Output()
	if err != nil {
		return err
	}

	containerID := strings.TrimSpace(string(out))
	fmt.Println("Docker container:", containerID)

	// Record the image ID: CRIU restore on the destination requires a rootfs
	// built from the *same* image build (identical binaries), so the receiver
	// compares this against its own bundle's image ID before restoring.
	imageID := ""
	if idOut, idErr := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", image).Output(); idErr == nil {
		imageID = strings.TrimSpace(string(idOut))
		fmt.Println("Image ID:", imageID)
	} else {
		fmt.Println("Warning: could not resolve image ID:", idErr)
	}

	// export rootfs
	fmt.Println("Exporting filesystem...")

	exportCmd := exec.CommandContext(ctx,
		"docker", "export", containerID,
	)

	file, err := os.Create("rootfs.tar")
	if err != nil {
		return err
	}

	exportCmd.Stdout = file
	exportCmd.Stderr = os.Stderr

	if err := exportCmd.Run(); err != nil {
		return err
	}

	// create rootfs directory: stateful bundles extract the image into
	// lowerdir and mount an overlay at "rootfs" ;
	// non-stateful bundles keep the flat rootfs (restart baseline).
	rootfsBase := "rootfs"
	if stateful {
		rootfsBase = overlayLowerDir
	}
	if err := os.MkdirAll(rootfsBase, 0755); err != nil {
		return err
	}

	// extract rootfs
	fmt.Println("Extracting rootfs...")

	untar := exec.CommandContext(ctx,
		"tar", "-xf", "rootfs.tar",
		"-C", rootfsBase,
	)

	untar.Stdout = os.Stdout
	untar.Stderr = os.Stderr

	if err := untar.Run(); err != nil {
		return err
	}

	os.Remove("rootfs.tar")

	if stateful {
		fmt.Println("Mounting overlay rootfs (M4)...")
		if err := os.MkdirAll(overlayUpperDir, 0755); err != nil {
			return err
		}
		if err := os.MkdirAll(overlayWorkDir, 0755); err != nil {
			return err
		}
		if err := os.MkdirAll("rootfs", 0755); err != nil {
			return err
		}
		if err := MountOverlay(); err != nil {
			return err
		}
	}

	// data dir created post-mount so it lands in upperdir for stateful
	// bundles (runtime-written volume, the M4 migration payload) and in the
	// flat tree otherwise.
	if wl != nil && wl.DataDir != "" {
		dataDir := filepath.Join("rootfs", filepath.FromSlash(wl.DataDir))
		if err := os.MkdirAll(dataDir, 0777); err != nil {
			return err
		}
		// The image may already ship the directory owned by its service user
		// (redis:7.4-alpine: /data 999:755). The workload runs as root without
		// CAP_DAC_OVERRIDE, so it could not write there: Redis AOF exits at
		// start ("Can't open or create append-only dir"), which broke template
		// synthesis on a fresh destination. The runner already does this for
		// the source bundle (workloads.py post_create); do it for every bundle.
		if err := os.Chmod(dataDir, 0777); err != nil {
			return err
		}
	}

	// generate OCI config
	fmt.Println("Generating OCI config")

	spec := exec.CommandContext(ctx, "runc", "spec")
	spec.Stdout = os.Stdout
	spec.Stderr = os.Stderr

	if err := spec.Run(); err != nil {
		return err
	}

	fmt.Println("Bundle created successfully")

	// create StatePod config
	if err := CreateConfig(containerID, image, imageID, wl, stateful, deterministic, subnet, gateway, ip, hostPort); err != nil {
		return err
	}

	// deterministic launch: ship the ASLR-off wrapper inside the rootfs
	if deterministic {
		fmt.Println("Installing deterministic-launch wrapper (ASLR off)...")
		if err := installNrand(rootfsBase); err != nil {
			return err
		}
	}

	// Provision the workload's files into the lower layer and point the OCI
	// config at its command (a plain container with no workload is left as
	// runc's default spec).
	if wl != nil {
		if len(wl.Provision) > 0 {
			fmt.Println("Provisioning workload files...")
			if err := provisionFiles(rootfsBase, wl.Provision); err != nil {
				return err
			}
		}
		fmt.Printf("Configuring %s workload...\n", wl.Name)
		if err := patchOCIConfig(wl, deterministic); err != nil {
			return err
		}
	}

	// write README
	if err := writeReadme(image); err != nil {
		return err
	}

	fmt.Println("Container created successfully. Run `statepod start` to start the container.")

	return nil
}
