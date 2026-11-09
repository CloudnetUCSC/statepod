package container

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/pipeline"
	"github.com/CloudnetUCSC/statepod/internal/workload"
)

// ManifestName is the page-hash manifest stored alongside a template dump
// : built once at synthesis, served to sources ahead of burst.
const ManifestName = "manifest.spm"

// TemplateMeta describes a synthesized baseline stored under
// ~/.statepod/templates/<digest>/ (the destination-local
// cold-start dump every future manifest/reconciliation is built from).
type TemplateMeta struct {
	Image          string `json:"image"`
	ImageID        string `json:"image_id"`
	Workload       string `json:"workload"`
	Deterministic  bool   `json:"deterministic"`
	CreatedAt      string `json:"created_at"`
	BaselineDumpID string `json:"baseline_dump_id,omitempty"`
	ManifestPages  int    `json:"manifest_pages,omitempty"`
}

// TemplatesDir returns the root of the template store.
func TemplatesDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".statepod", "templates"), nil
}

// TemplateDir returns the stored template directory for an image ID (templates
// are content-addressed by image digest). Used by the destination
// reassembly path to load the local baseline.
func TemplateDir(imageID string) (string, error) {
	return templateDigestDir(imageID)
}

// TemplateManifestBytes reads the stored page-hash manifest for an image
// digest, for the pre-burst manifest exchange. Errors if no
// template has been synthesized for that digest.
func TemplateManifestBytes(imageID string) ([]byte, error) {
	dir, err := templateDigestDir(imageID)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return nil, fmt.Errorf("no manifest for image %s (run `statepod template generate` on the destination first): %w", imageID, err)
	}
	return data, nil
}

// templateDigestDir maps an image ID (sha256:<hex>) to its store directory.
func templateDigestDir(imageID string) (string, error) {
	root, err := TemplatesDir()
	if err != nil {
		return "", err
	}
	digest := strings.TrimPrefix(imageID, "sha256:")
	if digest == "" {
		return "", fmt.Errorf("empty image digest")
	}
	return filepath.Join(root, digest), nil
}

// GenerateTemplate cold-starts a throwaway instance of image with
// deterministic launch, waits for the readiness point (same WaitReady hook as
// source start), CRIU-dumps it, stores the dump per image digest, and
// discards the instance. Returns the template directory (§3.1–3.2).
func GenerateTemplate(ctx context.Context, image string, wl *workload.Spec) (string, error) {
	origDir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	workDir, err := os.MkdirTemp("", "statepod-template-*")
	if err != nil {
		return "", err
	}
	defer func() {
		os.Chdir(origDir)
		if err := exec.Command("sudo", "rm", "-rf", workDir).Run(); err != nil {
			fmt.Printf("Warning: failed to remove template work dir %s: %v\n", workDir, err)
		}
	}()

	if err := os.Chdir(workDir); err != nil {
		return "", err
	}

	// Distinct IP so the readiness probe cannot hit a concurrently running
	// container (e.g. a restored instance on the default 10.10.0.10), and
	// host_port -1 so no DNAT rules are created or disturbed.
	fmt.Println("Template synthesis: cold-starting throwaway instance in", workDir)
	if err := CreateContainer(ctx, image, wl, wl != nil && wl.Stateful, true, "10.10.0.0/24", "10.10.0.1", "10.10.0.20", -1); err != nil {
		return "", fmt.Errorf("template bundle creation failed: %w", err)
	}

	cfg, err := ReadConfig()
	if err != nil {
		return "", err
	}
	if cfg.ImageID == "" {
		return "", fmt.Errorf("image ID unresolved for %s — template store is keyed by digest", image)
	}
	if cfg.Stateful {
		// Registered after the workDir chdir/rm-rf cleanup above, so it runs
		// first (LIFO defers): unmount before the bundle dir is removed, or
		// the host is left with a stale mount entry pointing at nothing.
		defer UnmountOverlay()
	}

	// Start brings up the container and blocks on the readiness point
	// (WaitReady) — identical anchor to a real source-side start.
	if err := Start(ctx, "", false); err != nil {
		return "", fmt.Errorf("template instance start failed: %w", err)
	}
	// The throwaway must never survive this function, even on dump failure.
	defer exec.Command("sudo", "runc", "delete", "-f", cfg.ContainerID).Run()

	dumpDir := filepath.Join(workDir, "template-dump")
	fmt.Println("Dumping template instance at readiness point...")
	dump := exec.CommandContext(ctx, "sudo", "runc", "checkpoint", cfg.ContainerID, "--image-path", dumpDir)
	dump.Stdout = os.Stdout
	dump.Stderr = os.Stderr
	if err := dump.Run(); err != nil {
		return "", fmt.Errorf("template checkpoint failed: %w", err)
	}

	destDir, err := templateDigestDir(cfg.TemplateKey())
	if err != nil {
		return "", err
	}
	if err := exec.CommandContext(ctx, "sudo", "rm", "-rf", destDir).Run(); err != nil {
		return "", err
	}
	if err := exec.CommandContext(ctx, "sudo", "mkdir", "-p", filepath.Dir(destDir)).Run(); err != nil {
		return "", err
	}
	if err := exec.CommandContext(ctx, "sudo", "cp", "-a", dumpDir, destDir).Run(); err != nil {
		return "", fmt.Errorf("failed to store template at %s: %w", destDir, err)
	}
	// The dump is written by `sudo runc checkpoint` (root-owned). Hand the
	// stored copy back to the invoking user so the reconciliation pipeline —
	// manifest serving and reassembly, both non-root — can read the pages and
	// write the manifest/meta sidecars. The template dir is only ever parsed by
	// statepod (never restored in place), so user ownership is safe.
	if u, err := user.Current(); err == nil {
		if err := exec.CommandContext(ctx, "sudo", "chown", "-R", u.Uid+":"+u.Gid, destDir).Run(); err != nil {
			return "", fmt.Errorf("failed to chown template store: %w", err)
		}
	}

	// Build and store the page-hash manifest (§3.3): the only
	// pre-burst WAN payload. Content-addressed by image digest via destDir.
	base, err := pipeline.LoadBaseline(destDir, cfg.TemplateKey())
	if err != nil {
		return "", fmt.Errorf("building baseline from template: %w", err)
	}
	manifest := base.Manifest()
	mf, err := os.Create(filepath.Join(destDir, ManifestName))
	if err != nil {
		return "", err
	}
	if err := manifest.Encode(mf); err != nil {
		mf.Close()
		return "", fmt.Errorf("encoding manifest: %w", err)
	}
	mf.Close()
	fmt.Printf("Manifest: %d baseline pages, dump id %s\n", base.NumPages(), base.DumpID)

	workloadName := ""
	if wl != nil {
		workloadName = wl.Name
	}
	meta := TemplateMeta{
		Image:          image,
		ImageID:        cfg.ImageID,
		Workload:       workloadName,
		Deterministic:  true,
		CreatedAt:      time.Now().Format(time.RFC3339),
		BaselineDumpID: base.DumpID,
		ManifestPages:  base.NumPages(),
	}
	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(destDir, "meta.json"), metaData, 0644); err != nil {
		return "", fmt.Errorf("failed to write template meta: %w", err)
	}

	fmt.Println("Template stored at", destDir)
	return destDir, nil
}
