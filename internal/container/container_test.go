package container

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CloudnetUCSC/statepod/internal/workload"
)

// setupTestEnv creates a temporary directory and sets it as the working directory
func setupTestEnv(t *testing.T) (string, func()) {
	// Create a temp directory
	tempDir, err := os.MkdirTemp("", "statepod-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Save current working directory
	originalWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// Change to temp directory
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Failed to change working directory: %v", err)
	}

	// Return cleanup function
	return tempDir, func() {
		os.Chdir(originalWd)
		os.RemoveAll(tempDir)
	}
}

func TestConfigManagement(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	// 1. Create Config
	err := CreateConfig("test-container-id", "busybox:latest", "sha256:testimageid", nil, false, false, "10.10.0.0/24", "10.10.0.1", "10.10.0.10", 6380)
	if err != nil {
		t.Fatalf("CreateConfig failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat("statepod.json"); os.IsNotExist(err) {
		t.Fatalf("statepod.json was not created")
	}

	// 2. Read Config
	cfg, err := ReadConfig()
	if err != nil {
		t.Fatalf("ReadConfig failed: %v", err)
	}

	if cfg.ContainerID != "test-container-id" {
		t.Errorf("Expected ContainerID 'test-container-id', got '%s'", cfg.ContainerID)
	}
	if cfg.DockerSource != "busybox:latest" {
		t.Errorf("Expected DockerSource 'busybox:latest', got '%s'", cfg.DockerSource)
	}
	if cfg.CurrentState != "stopped" {
		t.Errorf("Expected CurrentState 'stopped', got '%s'", cfg.CurrentState)
	}

	// 3. Update Config
	cfg.CurrentState = "running"
	cfg.CurrentPID = 1234

	err = UpdateConfig(cfg)
	if err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}

	// Ensure changes persisted
	updatedCfg, err := ReadConfig()
	if err != nil {
		t.Fatalf("ReadConfig after update failed: %v", err)
	}

	if updatedCfg.CurrentState != "running" {
		t.Errorf("Expected CurrentState 'running', got '%s'", updatedCfg.CurrentState)
	}
	if updatedCfg.CurrentPID != 1234 {
		t.Errorf("Expected CurrentPID 1234, got %d", updatedCfg.CurrentPID)
	}
}

func TestWriteReadme(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	image := "ubuntu:latest"
	err := writeReadme(image)
	if err != nil {
		t.Fatalf("writeReadme failed: %v", err)
	}

	content, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("Failed to read README.md: %v", err)
	}

	contentStr := string(content)
	if !contains(contentStr, image) {
		t.Errorf("README.md does not contain the image name '%s'", image)
	}
}

func TestCreateNodeServerFiles(t *testing.T) {
	tempDir, cleanup := setupTestEnv(t)
	defer cleanup()

	node, _ := workload.Builtin("node")
	err := provisionFiles("rootfs", node.Provision)
	if err != nil {
		t.Fatalf("provisionFiles failed: %v", err)
	}

	serverPath := filepath.Join(tempDir, "rootfs", "app", "server.js")
	if _, err := os.Stat(serverPath); os.IsNotExist(err) {
		t.Fatalf("server.js was not created at expected path: %s", serverPath)
	}
}

func TestWrapArgs(t *testing.T) {
	args := []string{"redis-server", "--dir", "/data"}

	plain := wrapArgs(args, false)
	if len(plain) != 3 || plain[0] != "redis-server" {
		t.Errorf("wrapArgs(false) should leave args untouched, got %v", plain)
	}

	det := wrapArgs(args, true)
	if len(det) != 4 || det[0] != "/"+nrandRootfsPath || det[1] != "redis-server" {
		t.Errorf("wrapArgs(true) should prepend the nrand wrapper, got %v", det)
	}
}

// Helper to check if a string contains a substring
func contains(s, substr string) bool {
	// simple strings.Contains implementation
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// NOTE: We don't write pure unit tests for Start(), Stop(), CreateContainer() here
// because they execute destructive `sudo runc` / `docker` commands that require privileges
// and a very specific host environment. Those are better suited for the E2E tests.
