package migration

import (
	"context"
	"os"
	"os/exec"
)

// criuEnvVars are CRIU options that exist only as environment variables. sudo
// resets the environment, so a value set for statepod would never reach the
// criu that runc starts; sudoRunc forwards them explicitly.
//
// CRIU_TRACK_SHMEM turns on dirty tracking of anonymous shared memory between
// pre-dump rounds (criu/shmem.c is_shmem_tracking_en; off by default). Without
// it every shared page with data is dumped again in the final dump: PostgreSQL's
// shared_buffers made its final dump 83 MB against ~0.9 MB per pre-dump round
// (2026-10-26). The runner sets it for every pre-copy variant alike.
var criuEnvVars = []string{"CRIU_TRACK_SHMEM"}

// sudoRunc returns `sudo [env K=V ...] <argv...>` for an argv that starts with
// "runc".
func sudoRunc(ctx context.Context, argv ...string) *exec.Cmd {
	full := []string{}
	for _, k := range criuEnvVars {
		if v, ok := os.LookupEnv(k); ok {
			if len(full) == 0 {
				full = append(full, "env")
			}
			full = append(full, k+"="+v)
		}
	}
	return exec.CommandContext(ctx, "sudo", append(full, argv...)...)
}
