"""Break `runc restore` (CRIU) into phases from restore.log.

CRIU timestamps every log line `(SS.UUUUUU)` relative to its own start, so the phase
durations need no clock translation. Phases are the intervals between milestone lines
(first occurrence of each regex, in log order); a milestone that is absent from a log
is skipped and its time falls into the next phase, never invented. Measured on the
Redis checkpoint (CRIU 4.2, runc 1.3.4, 2026-10-05; 0.33 s in total):

  read_images        start -> pre-restore scripts      image parse, cgroup yard
  netns_fork         -> clone3 of the init task        net ns fd, mount images read
  pre_ns             -> cgns prefix                    time ns, setup-namespaces hook
  cgroup_attach      -> "Move into"                    runc's cgroup setup in the
                                                       setup-namespaces notification
  ipc_uts            -> first mount operation          hostname, IPC namespace
  mount_tree         -> mount-tree root move           ~120 mounts (masked/read-only
                                                       paths, /proc, /sys, tmpfs)
  post_mount         -> VMA list read                  post-setup-namespaces hook
  tasks_and_memory   -> first task "Restored"          fds, VMAs, page restore
  post_restore       -> network unlocked               post-restore hook, ghosts
  finalize_resume    -> "Restore finished"             restorer finalisation and ptrace
                                                       resume of the threads; ~90 ms of
                                                       it is SILENT in the log (no line
                                                       between the post-restore hook and
                                                       the per-thread seccomp lines), so
                                                       it cannot be split further without
                                                       strace/perf

Usage: python criu_restore_profile.py <restore.log>...   (prints JSON per file)
"""
import json
import re
import sys
from pathlib import Path

LINE = re.compile(r"^\((\d+\.\d+)\) (.*)$")

MILESTONES = [
    ("read_images", r"Running pre-restore scripts"),
    ("netns_fork", r"Creating process using clone3|Forking task"),
    ("pre_ns", r"cg: setting cgns prefix"),
    ("cgroup_attach", r"cg: Move into"),
    ("ipc_uts", r"mnt-v2: .*(can_mount_now|Mounting)|mnt: Start with"),
    ("mount_tree", r"mnt: Move the root to /tmp/\.criu\.mntns\.\w+/\d+-"),
    ("post_mount", r"Found \d+ VMAs in image"),
    ("tasks_and_memory", r"pie: \d+: \d+: Restored"),
    ("post_restore", r"net: Unlock network"),
    ("finalize_resume", r"Restore finished successfully"),
]


def parse(text):
    rows = []
    for line in text.splitlines():
        m = LINE.match(line.strip("\r"))
        if m:
            rows.append((float(m.group(1)), m.group(2)))
    return rows


def profile(text):
    rows = parse(text)
    out = {"total_ms": None, "phases_ms": {}, "mounts": 0, "finished": False}
    if not rows:
        return out
    out["finished"] = any("Restore finished successfully" in x for _, x in rows)
    out["total_ms"] = round(rows[-1][0] * 1000.0, 1)
    out["mounts"] = sum(1 for _, x in rows if re.search(r"mnt-v2: .*Mounting ", x))
    prev_t = rows[0][0]
    idx = 0
    for name, pat in MILESTONES:
        rx = re.compile(pat)
        hit = None
        for i in range(idx, len(rows)):
            if rx.search(rows[i][1]):
                hit = i
                break
        if hit is None:
            continue                       # absent milestone: its time joins the next phase
        t = rows[hit][0]
        out["phases_ms"][name] = round((t - prev_t) * 1000.0, 1)
        prev_t, idx = t, hit
    return out


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print(p, json.dumps(profile(Path(p).read_text(errors="replace"))))
