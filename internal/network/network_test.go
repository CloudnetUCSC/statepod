package network

import (
	"context"
	"testing"
)

// We can test runCmd with a simple command that doesn't modify the host
func TestRunCmd(t *testing.T) {
	ctx := context.Background()

	// Test a successful command
	err := runCmd(ctx, "echo", "hello statepod")
	if err != nil {
		t.Fatalf("runCmd failed on a valid command: %v", err)
	}

	// Test a failing command
	err = runCmd(ctx, "ls", "/directory/that/does/not/exist")
	if err == nil {
		t.Fatal("runCmd succeeded on an invalid command, expected an error")
	}
}
