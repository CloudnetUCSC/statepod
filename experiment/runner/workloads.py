"""Workload registry for the experiment runner.

Everything the runner needs to know about a workload lives in one class here,
replacing the `args.workload == "redis"` branches that were spread through
run_experiment.py. The engine side of the same idea is internal/workload
(Spec): the runner passes `create_flags` to `statepod create`, and the Spec
the engine resolves from that flag decides command, port and readiness.

Adding a workload (PostgreSQL, a JVM service, the synthetic knob workload) =
subclass Workload, implement `seed` and `verify`, register it in WORKLOADS.
`rx` below is the run_experiment module (passed in to avoid a circular
import); the methods use rx.ssh / rx.try_out / rx.wait_* exactly as the
inline code did before the refactor.
"""
import datetime
import shlex
import time
from pathlib import Path

DATASET_SIZES = {"small": 500, "medium": 5000, "large": 50000, "xlarge": 500000}  # xlarge: Redis only


class Workload:
    name = ""
    image_key = "image"        # key under `experiment:` in hosts.yaml
    image_default = ""
    create_flags = ""          # passed to `statepod create` / `template generate`
    probe_kind = "tcp"         # client probe / load client protocol: resp | http | tcp
    stateful = False           # migrates live state (needs a destination template for synth modes)
    supports_lostwrite = False

    def image(self, cfg):
        return cfg.get("experiment", {}).get(self.image_key, self.image_default)

    def prepare(self, rx, cfg, args):
        """Once per run, before bundles are created (e.g. distribute a spec file)."""

    def flags(self, rx, cfg, args):
        """Workload flags for `statepod create` / `template generate`."""
        return self.create_flags

    def post_create(self, rx, target, wd):
        """Hook after `statepod create` on a host (permissions fix-ups etc.)."""

    def wait_ready(self, rx, src, port, src_wd):
        raise NotImplementedError

    def seed(self, rx, cfg, args, port):
        """Write the dataset on the source; return an opaque state (the canary)."""
        return None

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        raise NotImplementedError


class Redis(Workload):
    name = "redis"
    image_default = "redis:7.4-alpine"
    create_flags = "--workload redis"
    probe_kind = "resp"
    stateful = True
    supports_lostwrite = True

    def post_create(self, rx, target, wd):
        # Pre-existing gap, unrelated to migration mode: the flat
        # `docker export` rootfs ships rootfs/data with stale
        # ownership from image extraction (observed as uid "lxd", not
        # root) and mode 0755 -- the container's own root can't write
        # there, so redis-cli SAVE fails silently (bare "ERR", no
        # message) and volume-sync migrations carry zero bytes.
        rx.ssh(target, f"sudo chmod -R a+rwX {wd}/rootfs/data", check=False, timeout=30)

    def wait_ready(self, rx, src, port, src_wd):
        if not rx.wait_redis(src, f"-h 127.0.0.1 -p {port}"):
            log = rx.try_out(src, f"tail -20 {src_wd}/start.log")
            raise RuntimeError(f"source Redis not answering on 127.0.0.1:{port} after "
                               f"start; start.log tail:\n{log}")

    def seed(self, rx, cfg, args, port):
        src = cfg["source"]["ssh"]
        canary = datetime.datetime.now().isoformat()
        key_count = DATASET_SIZES[args.dataset]
        expected_dbsize = str(key_count + 1)  # + migration_canary
        # EVAL, not DEBUG POPULATE: redis 7 ships with the DEBUG command disabled
        # (and redis-cli exits 0 on error replies, silently skipping the seed).
        seed = f"for i=1,{key_count} do redis.call('set','key:'..i,i) end"
        out = rx.ssh(src, f"redis-cli -h 127.0.0.1 -p {port} set migration_canary '{canary}' && "
                          f"redis-cli -h 127.0.0.1 -p {port} eval \"{seed}\" 0 && "
                          f"redis-cli -h 127.0.0.1 -p {port} dbsize", timeout=60)
        dbsize = out.stdout.strip().splitlines()[-1]
        if dbsize != expected_dbsize:
            raise RuntimeError(f"seeding failed: source dbsize={dbsize!r}, expected {expected_dbsize}")
        print(f"  source Redis up, seeded {key_count} keys [{args.dataset}] (canary {canary})")
        return canary

    def verify(self, rx, cfg, args, canary, ip, port, attempts):
        src = cfg["source"]["ssh"]
        expect_state = not (args.mode in ("sync", "sync-rsync") and args.no_volume_sync)
        key_count = DATASET_SIZES[args.dataset]
        expected_dbsize = str(key_count + 1)
        res = {"canary_sent": canary, "expect_state_preserved": expect_state}
        for attempt in range(attempts):
            res["canary_received"] = rx.try_out(
                src, f"redis-cli -h {ip} -p {port} get migration_canary")
            res["dbsize"] = rx.try_out(src, f"redis-cli -h {ip} -p {port} dbsize")
            if expect_state:
                if getattr(args, "lost_write", False):
                    # the lost-write client only ever adds keys
                    res["ok"] = (res["canary_received"] == canary and res["dbsize"].isdigit()
                                 and int(res["dbsize"]) >= int(expected_dbsize))
                else:
                    res["ok"] = (res["canary_received"] == canary and res["dbsize"] == expected_dbsize)
            else:
                res["ok"] = res["dbsize"].isdigit()  # destination reachable and serving
            if res["ok"] or attempt == attempts - 1:
                break
            time.sleep(1)
        res["verify_attempts"] = attempt + 1
        return res


class RedisAof(Redis):
    """Redis with append-only persistence on (`--appendonly yes --appendfsync everysec`,
    ): the realistic production configuration, and what the
    fair restart baseline needs (the data directory then holds an AOF the restarted
    server replays). Everything else, seeding, verification and the lost-write test, is
    Redis'. Same pinned image; the Spec is named "redis" so engine-side special cases
    (volume sync SAVE, readiness) apply, and its different args give it its own
    destination template."""
    name = "redis-aof"
    SPEC_REMOTE = "/tmp/statepod_redis_aof_spec.json"

    def spec(self, args):
        return {"name": "redis", "description": "Redis with AOF persistence (appendfsync everysec)",
                "image": self.image_default, "port": 6379,
                "args": ["redis-server", "--dir", "/data", "--appendonly", "yes", "--appendfsync", "everysec"],
                "ready": {"kind": "resp"}, "data_dir": "data", "stateful": True}

    def prepare(self, rx, cfg, args):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(self.spec(args), f, indent=1)
            local = f.name
        for role in ("source", "destination"):
            rx.scp_to(cfg[role]["ssh"], local, self.SPEC_REMOTE)

    def flags(self, rx, cfg, args):
        return f"--workload {self.SPEC_REMOTE}"


class Node(Workload):
    name = "node"
    image_key = "node_image"
    image_default = "node:20-alpine"
    create_flags = "--workload node"
    probe_kind = "http"
    stateful = False

    def wait_ready(self, rx, src, port, src_wd):
        # stateless HTTP workload, nothing to seed -- readiness is enough.
        if not rx.wait_http(src, "127.0.0.1", port):
            log = rx.try_out(src, f"tail -20 {src_wd}/start.log")
            raise RuntimeError(f"source Node server not answering on 127.0.0.1:{port} after "
                               f"start; start.log tail:\n{log}")
        print("  source Node server up")

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        # restart-based migration carries no state -- just confirm the
        # destination is serving again after the dispatcher's restart path.
        src = cfg["source"]["ssh"]
        res = {}
        for attempt in range(attempts):
            code = rx.try_out(src, f"curl -s -o /dev/null -w '%{{http_code}}' "
                                   f"--max-time 3 http://{ip}:{port}/")
            res = {"http_status": code, "ok": code == "200"}
            if res["ok"] or attempt == attempts - 1:
                break
            time.sleep(1)
        res["verify_attempts"] = attempt + 1
        return res


class Synth(Workload):
    """Controlled synthetic workload (workloads/synth).

    Knobs come from repeated `--wl-param key=value` (see synth_workload.c):
    resident-mb, shared-frac, entropy, dirty-pps, ws-pages, file-mb, files-pps,
    seed. The Spec is generated from them, so every parameter set gets its own
    destination template (the template key includes a spec fingerprint).
    """
    name = "synth"
    image_key = "synth_image"
    image_default = "statepod-synth:1"
    probe_kind = "line"
    stateful = True
    SPEC_REMOTE = "/tmp/statepod_synth_spec.json"
    KNOBS = ("resident-mb", "shared-frac", "entropy", "dirty-pps", "ws-pages",
             "file-mb", "files-pps", "seed")

    @staticmethod
    def params(args):
        p = {"resident-mb": "64", "shared-frac": "1.0", "entropy": "random",
             "dirty-pps": "0", "ws-pages": "0", "file-mb": "0", "files-pps": "0", "seed": "12345"}
        for kv in getattr(args, "wl_param", []) or []:
            k, _, v = kv.partition("=")
            if k not in p:
                raise SystemExit(f"unknown --wl-param {k!r}; synth knobs: {', '.join(Synth.KNOBS)}")
            p[k] = v
        return p

    def spec(self, args):
        argv = ["/synth_workload", "--port", "9000"]
        for k, v in self.params(args).items():
            if k == "ws-pages" and v == "0":
                continue
            argv += [f"--{k}", v]
        return {"name": "synth", "description": "synthetic knob workload", "port": 9000,
                "args": argv, "ready": {"kind": "tcp"}, "data_dir": "data", "stateful": True}

    def prepare(self, rx, cfg, args):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(self.spec(args), f, indent=1)
            local = f.name
        for role in ("source", "destination"):
            rx.scp_to(cfg[role]["ssh"], local, self.SPEC_REMOTE)
        print(f"  synth spec distributed: {self.params(args)}")

    def flags(self, rx, cfg, args):
        return f"--workload {self.SPEC_REMOTE}"

    @staticmethod
    def _cmd(ip, port, line, timeout=60):
        return ("python3 -c \"import socket;s=socket.create_connection(('%s',%s),10);s.settimeout(%d);"
                "s.sendall(b'%s\\n');print(s.makefile().readline().strip())\"" % (ip, port, timeout, line))

    @staticmethod
    def parse_ok(out):
        """'OK bad=0 pages=N gen=G files=M filebad=0' -> dict, or None."""
        parts = out.strip().split()
        if not parts or parts[0] != "OK":
            return None
        return {k: int(v) for k, v in (p.split("=") for p in parts[1:])}

    def wait_ready(self, rx, src, port, src_wd):
        for _ in range(60):
            out = rx.try_out(src, self._cmd("127.0.0.1", port, "PING", 5))
            if out.strip() == "PONG":
                return
            time.sleep(1)
        log = rx.try_out(src, f"tail -20 {src_wd}/start.log")
        raise RuntimeError(f"synthetic workload not answering on 127.0.0.1:{port}; start.log tail:\n{log}")

    def seed(self, rx, cfg, args, port):
        src = cfg["source"]["ssh"]
        v = self.parse_ok(rx.try_out(src, self._cmd("127.0.0.1", port, "VERIFY")))
        if not v or v.get("bad") or v.get("filebad"):
            raise RuntimeError(f"synthetic workload failed its own VERIFY before migration: {v}")
        print(f"  synth source up: {v['pages']} pages, {v['files']} files, VERIFY clean")
        return {"verify_pre": v, "params": self.params(args)}

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        src = cfg["source"]["ssh"]
        pre = (state or {}).get("verify_pre", {})
        res = {"verify_pre": pre, "expect_state_preserved": True}
        for attempt in range(attempts):
            v = self.parse_ok(rx.try_out(src, self._cmd(ip, port, "VERIFY")))
            res["verify_post"] = v
            # Exact integrity: every page and file matches its generating
            # function, and no page generation went backwards.
            res["ok"] = bool(v and v["bad"] == 0 and v["filebad"] == 0
                             and v["pages"] == pre.get("pages") and v["gen"] >= pre.get("gen", 0))
            if res["ok"] or attempt == attempts - 1:
                break
            time.sleep(1)
        res["verify_attempts"] = attempt + 1
        return res


class SynthCpu(Synth):
    """CPU heterogeneity: the synthetic workload built with the --cpu-path knob (own
    image tag, so the plain synth cells keep their binary). `--wl-param cpu-path=auto|generic`
    (default auto). auto: the process picks the VAES code path at start when its CPU has VAES
    and keeps executing it; restored on a CPU without VAES it dies with an illegal instruction.
    The check is the process still advancing on the destination."""
    name = "synth-cpu"
    image_key = "synth_cpu_image"
    image_default = "statepod-synth-cpu:1"
    SPEC_REMOTE = "/tmp/statepod_synth_cpu_spec.json"
    KNOBS = Synth.KNOBS + ("cpu-path",)

    @staticmethod
    def params(args):
        p = {"resident-mb": "16", "shared-frac": "1.0", "entropy": "random", "dirty-pps": "0",
             "ws-pages": "0", "file-mb": "0", "files-pps": "0", "seed": "12345", "cpu-path": "auto"}
        for kv in getattr(args, "wl_param", []) or []:
            k, _, v = kv.partition("=")
            if k not in p:
                raise SystemExit(f"unknown --wl-param {k!r}; knobs: {', '.join(SynthCpu.KNOBS)}")
            p[k] = v
        return p

    def spec(self, args):
        sp = super().spec(args)
        sp["name"] = "synth-cpu"
        sp["image"] = self.image_default
        return sp

    @staticmethod
    def _cpu(rx, src, ip, port):
        out = rx.try_out(src, SynthCpu._cmd(ip, port, "CPU", 10))
        parts = out.split()
        if len(parts) == 3 and parts[0] == "OK":
            return {"path": parts[1].split("=")[1], "iters": int(parts[2].split("=")[1])}
        return None

    def seed(self, rx, cfg, args, port):
        st = super().seed(rx, cfg, args, port)
        time.sleep(1)
        st["cpu_pre"] = self._cpu(rx, cfg["source"]["ssh"], "127.0.0.1", port)
        print(f"  source cpu path: {st['cpu_pre']}")
        return st

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        res = super().verify(rx, cfg, args, state, ip, port, attempts)
        pre = (state or {}).get("cpu_pre") or {}
        time.sleep(2)
        post = self._cpu(rx, cfg["source"]["ssh"], ip, port)
        res["cpu_pre"], res["cpu_post"] = pre, post
        # the state check passing is not enough: the process must still be EXECUTING its chosen path
        res["cpu_advancing"] = bool(post and pre and post["path"] == pre["path"] and post["iters"] > pre["iters"])
        res["ok"] = bool(res.get("ok")) and res["cpu_advancing"]
        return res


class Postgres(Workload):
    """PostgreSQL 16 + pgbench: Debian/glibc, six processes, shared memory,
    real disk writes. The data directory lives in the overlay upper layer (the
    spec's `data_dir`), so M4 moves it and the plain baselines need `--fs-sync flat`
    (without it a checkpoint restores against missing data files).

    State = pgbench's schema at a chosen scale (`--dataset`: small s=1 ~15 MB,
    medium s=5 ~75 MB, large s=20 ~300 MB) after a few seconds of write load; the
    check is an aggregate over the whole schema read BEFORE the migration (on the
    source) and AFTER it (on the destination) and compared exactly. Everything runs
    through `runc exec` inside the container, so no client tools are needed on the
    VMs.
    """
    name = "postgres"
    image_key = "pg_image"
    image_default = "postgres:16"
    probe_kind = "pg"          # SSLRequest answered by the postmaster (a TCP connect succeeds while frozen)
    stateful = True
    SPEC_REMOTE = "/tmp/statepod_postgres_spec.json"
    SCALES = {"small": 1, "medium": 5, "large": 20}
    LOAD_SECONDS = 3
    CHECK_SQL = ("select count(*), coalesce(sum(abalance),0), sum(aid), "
                 "(select coalesce(sum(tbalance),0) from pgbench_tellers), "
                 "(select count(*) from pgbench_history) from pgbench_accounts")

    def prepare(self, rx, cfg, args):
        local = Path(__file__).resolve().parent.parent.parent / "workloads" / "postgres.json"
        for role in ("source", "destination"):
            rx.scp_to(cfg[role]["ssh"], local, self.SPEC_REMOTE)

    def flags(self, rx, cfg, args):
        return f"--workload {self.SPEC_REMOTE}"

    @staticmethod
    def _exec(rx, target, wd, cmd, timeout=300, check=False):
        """Run `cmd` (a shell command line) inside the container on `target`."""
        # cd first: a leading ~ in the workdir is expanded by the login shell, not by python
        cid = "$(python3 -c \"import json;print(json.load(open('statepod.json'))['container_id'])\")"
        return rx.ssh(target, f"cd {wd} && sudo runc exec {cid} sh -c {shlex.quote(cmd)}", check=check, timeout=timeout)

    def wait_ready(self, rx, src, port, src_wd):
        for _ in range(90):
            r = self._exec(rx, src, src_wd, "pg_isready -U postgres -h 127.0.0.1", timeout=30)
            if r.returncode == 0:
                return
            time.sleep(1)
        log = rx.try_out(src, f"tail -20 {src_wd}/start.log")
        raise RuntimeError(f"PostgreSQL not ready on the source; start.log tail:\n{log}")

    def _check(self, rx, target, wd):
        r = self._exec(rx, target, wd, f"psql -U postgres -h 127.0.0.1 -At -F, -c \"{self.CHECK_SQL}\" postgres", timeout=120)
        return r.stdout.strip().splitlines()[-1] if r.returncode == 0 and r.stdout.strip() else None

    def seed(self, rx, cfg, args, port):
        src, wd = cfg["source"]["ssh"], cfg["source"]["workdir"]
        scale = self.SCALES[args.dataset]
        r = self._exec(rx, src, wd, f"pgbench -U postgres -h 127.0.0.1 -i -q -s {scale} postgres", timeout=900)
        if r.returncode != 0:
            raise RuntimeError(f"pgbench -i failed: {r.stdout[-300:]} {r.stderr[-300:]}")
        r = self._exec(rx, src, wd, f"pgbench -U postgres -h 127.0.0.1 -c 4 -T {self.LOAD_SECONDS} -N postgres", timeout=120)
        if r.returncode != 0:
            raise RuntimeError(f"pgbench load failed: {r.stdout[-300:]} {r.stderr[-300:]}")
        before = self._check(rx, src, wd)
        if not before:
            raise RuntimeError("could not read the PostgreSQL state checksum on the source")
        print(f"  postgres source up, pgbench scale {scale} [{args.dataset}] + {self.LOAD_SECONDS}s load: {before}")
        return {"state_pre": before, "scale": scale}

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        dst, wd = cfg["destination"]["ssh"], cfg["destination"]["workdir"]
        pre = (state or {}).get("state_pre")
        res = {"state_pre": pre, "expect_state_preserved": True}
        for attempt in range(attempts):
            post = self._check(rx, dst, wd)
            res["state_post"] = post
            res["ok"] = bool(post) and post == pre
            if res["ok"] or attempt == attempts - 1:
                break
            time.sleep(1)
        res["verify_attempts"] = attempt + 1
        return res


class Jvm(Workload):
    """JVM HTTP service with a deterministic in-memory cache (workloads/jvm).

    JIT-compiled code, class metadata and a sizeable heap all migrate; most pages
    of a JVM are identical across instances of the same image (the expected best case
    for M1). `--dataset` sets the cache: small 5k x 4 KiB (~20 MB), medium 20k (~80 MB),
    large 50k (~200 MB). State check = GET /checksum (entries, CRC32 over every key and
    value, write count) read on the source after a burst of writes and on the destination
    after the migration, compared exactly; the service also verifies every value against
    its generating function. The Spec is generated per dataset (so each size gets its
    own destination template) and the client probe is the generic HTTP one.
    """
    name = "jvm"
    image_key = "jvm_image"
    image_default = "statepod-jvm:1"
    probe_kind = "http"
    stateful = True
    SPEC_REMOTE = "/tmp/statepod_jvm_spec.json"
    SIZES = {"small": 5000, "medium": 20000, "large": 50000}
    # Heap per dataset (MB), sized to the cache (entries x 4 KiB x ~1.3) plus headroom. Kept small so the
    # JVM's checkpoint (hundreds of MB) plus the restored process fit a ~1 GB destination. Metaspace and code cache are
    # capped for the same reason, and the young generation is kept small so eden is not touched wholesale.
    HEAP_MB = {"small": 64, "medium": 160, "large": 384}
    WRITES = 300

    def spec(self, args):
        return {"name": "jvm", "description": "JVM HTTP service with a deterministic in-memory cache",
                "image": self.image_default, "port": 8000,
                "args": ["java", f"-Xms{self.HEAP_MB[args.dataset]}m", f"-Xmx{self.HEAP_MB[args.dataset]}m",
                         "-Xmn24m", "-XX:MaxMetaspaceSize=64m", "-XX:ReservedCodeCacheSize=32m", "-Xss512k",
                         "-XX:+UseSerialGC", "-XX:-UsePerfData", "-cp", "/app",
                         "Server", "--entries", str(self.SIZES[args.dataset]), "--value-kb", "4"],
                "env": ["PATH=/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                        "JAVA_HOME=/opt/java/openjdk"],
                "ready": {"kind": "http", "path": "/health"}, "stateful": True}

    def prepare(self, rx, cfg, args):
        import json
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(self.spec(args), f, indent=1)
            local = f.name
        for role in ("source", "destination"):
            rx.scp_to(cfg[role]["ssh"], local, self.SPEC_REMOTE)

    def flags(self, rx, cfg, args):
        return f"--workload {self.SPEC_REMOTE}"

    @staticmethod
    def _get(rx, src, ip, port, path, timeout=60):
        r = rx.ssh(src, f"curl -s --max-time {timeout} http://{ip}:{port}{path}", check=False, timeout=timeout + 15)
        return r.stdout.strip() if r.returncode == 0 else ""

    def wait_ready(self, rx, src, port, src_wd):
        if not rx.wait_http(src, "127.0.0.1", port):
            log = rx.try_out(src, f"tail -20 {src_wd}/start.log")
            raise RuntimeError(f"JVM service not answering on 127.0.0.1:{port}; start.log tail:\n{log}")

    def seed(self, rx, cfg, args, port):
        src = cfg["source"]["ssh"]
        # writes make the state more than the initial deterministic fill
        rx.ssh(src, f"for i in $(seq 1 {self.WRITES}); do curl -s http://127.0.0.1:{port}/put?i=$((i*7)) >/dev/null; done",
               timeout=300)
        pre = self._get(rx, src, "127.0.0.1", port, "/checksum")
        if not pre or "MISMATCH" in pre:
            raise RuntimeError(f"JVM service failed its own checksum before migration: {pre!r}")
        print(f"  JVM source up: {self.SIZES[args.dataset]} entries [{args.dataset}] + {self.WRITES} writes: {pre}")
        return {"checksum_pre": pre}

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        src = cfg["source"]["ssh"]
        pre = (state or {}).get("checksum_pre")
        res = {"checksum_pre": pre, "expect_state_preserved": True}
        for attempt in range(attempts):
            post = self._get(rx, src, ip, port, "/checksum")
            res["checksum_post"] = post
            res["ok"] = bool(post) and post == pre
            if res["ok"] or attempt == attempts - 1:
                break
            time.sleep(1)
        res["verify_attempts"] = attempt + 1
        return res


class Pyapp(Jvm):
    """The same HTTP cache service on CPython + glibc (workloads/pyapp): a third
    runtime and allocator. The protocol and the state check are the JVM workload's."""
    name = "pyapp"
    image_key = "pyapp_image"
    image_default = "statepod-pyapp:1"
    SPEC_REMOTE = "/tmp/statepod_pyapp_spec.json"

    def spec(self, args):
        return {"name": "pyapp", "description": "CPython HTTP service with a deterministic in-memory cache",
                "image": self.image_default, "port": 8000,
                "args": ["python", "-u", "/app/app.py", "--entries", str(self.SIZES[args.dataset]), "--value-kb", "4"],
                "env": ["PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
                        "PYTHONHASHSEED=0", "PYTHONDONTWRITEBYTECODE=1"],
                "ready": {"kind": "http", "path": "/health"}, "stateful": True}


class PyappOut(Pyapp):
    """pyapp whose process holds one long-lived connection to an EXTERNAL service
    (upstream_service.py on the destination host). Same image as pyapp plus the opt-in
    --upstream client, built as its own tag so the pyapp cells are untouched."""
    name = "pyapp-out"
    image_key = "pyapp_out_image"
    image_default = "statepod-pyapp-out:1"
    SPEC_REMOTE = "/tmp/statepod_pyapp_out_spec.json"
    upstream = ""

    def spec(self, args):
        sp = super().spec(args)
        sp["name"] = "pyapp-out"
        sp["image"] = self.image_default
        sp["args"] = sp["args"] + ["--upstream", self.upstream]
        return sp

    def prepare(self, rx, cfg, args):
        self.upstream = cfg["destination"].get("upstream_addr") or ""
        if not self.upstream:
            raise SystemExit("workload pyapp-out needs destination.upstream_addr (host:port reachable from BOTH "
                             "hosts) in hosts.yaml")
        super().prepare(rx, cfg, args)

    def verify(self, rx, cfg, args, state, ip, port, attempts):
        res = super().verify(rx, cfg, args, state, ip, port, attempts)
        raw = self._get(rx, cfg["source"]["ssh"], ip, port, "/outbound", timeout=20)
        try:
            import json
            res["outbound_app"] = json.loads(raw)
        except Exception:
            res["outbound_app"] = {"error": raw[:200]}
        return res


WORKLOADS = {w.name: w for w in (Redis(), RedisAof(), Node(), Synth(), SynthCpu(), Postgres(), Jvm(), Pyapp(), PyappOut())}


def get(name):
    try:
        return WORKLOADS[name]
    except KeyError:
        raise SystemExit(f"unknown workload {name!r}; known: {', '.join(sorted(WORKLOADS))}")
