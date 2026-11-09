"""Fault injection for the fault matrix.

    --fault TYPE@PHASE[:PARAM]

  TYPE   loss            netem packet loss (PARAM = percent, default 30) on the source's
                         egress for 20 s, on top of any bandwidth cap
         partition       drop the source's migration traffic for PARAM seconds (default
                         15; longer than the transport's 120 s idle deadline it becomes a
                         failure the migration must survive by falling back)
         kill-receiver   SIGKILL the destination's `statepod start` (the receiver)
         kill-restore    SIGKILL the destination's `criu restore` as soon as it appears
                         (a watcher runs ON the destination: an ssh round trip of 2 s
                         would miss a 0.3 s restore)
         hang-daemon     SIGSTOP the destination's `criu lazy-pages` daemon: the restored
                         process blocks on its next missing page (drain phase; reproduces a
                         stuck daemon, seen in 2 real runs, on demand)
  PHASE  precopy         the first pre-dump round after the first (loss / partition /
                         kill-receiver)
         final           the final dump / pause (loss / partition / kill-receiver)
         drain           the destination is serving and the source is still needed
                         (M5 page drain, stock criu-lazy)
         (kill-restore ignores the phase: restore only happens once)

The trigger is a marker line in source_migrate.log, so a fault lands at a protocol
phase, not at a wall-clock offset. Everything injected is undone in `stop()` even if
the run failed. `summary()` goes into the manifest.
"""
import re
import threading
import time

PHASE_MARKERS = {
    "precopy": re.compile(r"[Pp]re-dump round [1-9]"),
    # stock criu-lazy has no "Starting ... dump" line: its final transfer is the stock_images stream
    "final": re.compile(r"Starting final dump|Starting final freeze dump|Starting M5 dump|Streaming \./checkpoint/final as stock_images"),
    "drain": re.compile(r"Destination restored and serving|Destination serving; source-side downtime"),
}
TYPES = ("loss", "partition", "kill-receiver", "kill-restore", "hang-daemon")


def parse(spec):
    m = re.fullmatch(r"([a-z-]+)@([a-z]+)(?::([0-9.]+))?", spec.strip())
    if not m or m.group(1) not in TYPES or m.group(2) not in PHASE_MARKERS:
        raise SystemExit(f"bad --fault {spec!r}: want TYPE@PHASE[:PARAM], TYPE in {TYPES}, "
                         f"PHASE in {tuple(PHASE_MARKERS)}")
    return {"type": m.group(1), "phase": m.group(2), "param": float(m.group(3)) if m.group(3) else None}


class Injector:
    def __init__(self, rx, cfg, args, log_path, spec):
        self.rx, self.cfg, self.args, self.log_path = rx, cfg, args, log_path
        self.spec = parse(spec)
        self.raw = spec
        self.injected_at = None        # local (orchestrator) clock, like every time here that is subtracted
        self.undone = False
        self.detail = ""
        self._stop = threading.Event()
        self._thread = None
        self._rule = None              # iptables rule to delete
        self._watcher = None           # destination-side kill-restore watcher (Popen)

    # -- lifecycle ---------------------------------------------------------
    def start(self):
        if self.spec["type"] == "kill-restore":
            dst = self.cfg["destination"]["ssh"]
            self._watcher = self.rx.ssh_bg(
                dst,
                # the marker echoed below must NOT contain "criu"+"restore": this very command line is
                # visible to pgrep/pkill, and a self-match killed the watcher before it fired (2026-10-08)
                ": FAULTWATCH; while ! pgrep -f '[c]riu.*restore' >/dev/null; do sleep 0.02; done; "
                "sudo pkill -9 -f '[c]riu.*restore'; echo watcher-fired-ok",
                self.log_path.parent / "fault_watcher.log")
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=60)
        self._undo()

    # -- trigger -----------------------------------------------------------
    def _run(self):
        if self.spec["type"] == "kill-restore":
            # the watcher injects by itself; record when it reports
            while not self._stop.is_set():
                try:
                    if "watcher-fired-ok" in (self.log_path.parent / "fault_watcher.log").read_text(errors="replace"):
                        self.injected_at = time.time()
                        self.detail = "criu restore killed on the destination"
                        return
                except OSError:
                    pass
                time.sleep(0.2)
            return
        marker = PHASE_MARKERS[self.spec["phase"]]
        pos = 0
        while not self._stop.is_set():
            try:
                with open(self.log_path, "r", errors="replace") as f:
                    f.seek(pos)
                    chunk = f.read()
                    pos = f.tell()
            except OSError:
                chunk = ""
            if chunk and marker.search(chunk):
                self._inject()
                return
            time.sleep(0.1)

    # -- actions -----------------------------------------------------------
    def _inject(self):
        t = self.spec["type"]
        p = self.spec["param"]
        src, dst = self.cfg["source"]["ssh"], self.cfg["destination"]["ssh"]
        self.injected_at = time.time()
        if t == "loss":
            pct = p if p is not None else 30
            netem = f"{self.args.netem} loss {pct:g}%".strip()
            self.rx.apply_netem(self.cfg, netem)
            self.detail = f"netem: {netem}"
            self._stop.wait(20)
            self._undo()
        elif t == "partition":
            secs = p if p is not None else 15
            addr = self.rx.migrate_dest_addr(self.cfg)
            port = self.cfg["source"].get("migrate_listen_port", "8080")
            self._rule = f"OUTPUT -d {addr} -p tcp --dport {port} -j DROP"
            self.rx.ssh(src, f"sudo iptables -I {self._rule}", check=False)
            self.detail = f"iptables -I {self._rule} for {secs:g}s"
            self._stop.wait(secs)
            self._undo()
        elif t == "kill-receiver":
            self.rx.ssh(dst, "sudo pkill -9 -f '[s]tatepod start' || true", check=False)
            self.detail = "receiver SIGKILLed on the destination"
        elif t == "hang-daemon":
            self.rx.ssh(dst, "sudo pkill -STOP -f '[c]riu lazy-pages' || true", check=False)
            self.detail = "lazy-pages daemon SIGSTOPped on the destination"

    def _undo(self):
        if self.undone:
            return
        self.undone = True
        t = self.spec["type"]
        try:
            if t == "loss" and self.injected_at:
                self.rx.apply_netem(self.cfg, self.args.netem) if self.args.netem else self.rx.clear_netem(self.cfg)
            if self._rule:
                for _ in range(3):
                    r = self.rx.ssh(self.cfg["source"]["ssh"], f"sudo iptables -D {self._rule}", check=False)
                    if r.returncode != 0:
                        break          # rule gone: deleted (a second -D fails)
            if self._watcher is not None:
                self.rx.ssh(self.cfg["destination"]["ssh"], "pkill -f '[F]AULTWATCH' ; true", check=False)
                self._watcher.terminate()
        except Exception as e:                      # undo must never raise out of cleanup
            self.detail += f" (undo error: {type(e).__name__}: {e})"

    def release(self):
        """End of the run: a stuck daemon stays stuck through the verification (a real one does
        not recover by itself; unfreezing it at migrate's return hid the failure, 2026-11-01)."""
        if self.spec["type"] == "hang-daemon":
            self.rx.ssh(self.cfg["destination"]["ssh"], "sudo pkill -9 -f '[c]riu lazy-pages' || true", check=False)

    def summary(self):
        if self._watcher is not None and self.injected_at is None:
            # the poll thread can be stopped before it sees a late watcher report (criu-lazy, 2026-10-08)
            try:
                if "watcher-fired-ok" in (self.log_path.parent / "fault_watcher.log").read_text(errors="replace"):
                    self.injected_at = time.time()
                    self.detail = "criu restore killed on the destination"
            except OSError:
                pass
        return {"spec": self.raw, "injected": self.injected_at is not None, "detail": self.detail}
