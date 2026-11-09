/*
 * synth_workload: controlled synthetic workload with knobs.
 *
 * A small, static, single-threaded program with NO allocator or libc-startup
 * variability beyond what is listed, so the axes the evaluation sweeps are the
 * ONLY things that differ between runs:
 *
 *   --resident-mb N      resident anonymous memory (state size)
 *   --shared-frac F      fraction of pages whose content is deterministic and
 *                        therefore IDENTICAL across instances started with the
 *                        same parameters (they can hit the destination's
 *                        cold-start baseline); the rest is instance-random
 *                        (cannot). Expected M1 hit rate at readiness ~= F.
 *   --entropy MODE       content of the instance-specific pages:
 *                        random (default) | zero | repeat
 *   --dirty-pps R        pages rewritten per second (dirty rate)
 *   --ws-pages W         size of the dirtied working set, in pages (default all)
 *   --file-mb F          filesystem state written under /data at start (MB)
 *   --files-pps R        files rewritten per second (filesystem write rate)
 *   --seed S             seed of the deterministic content
 *   --port P             TCP command port (default 9000)
 *   --cpu-path M         off (default) | auto | generic. CPU heterogeneity: with auto the
 *                        process picks its code path ONCE at start from cpuid (VAES if the CPU has it)
 *                        and then keeps executing that path (one 256-bit VAESENC per loop pass), so a
 *                        checkpoint taken on a CPU with VAES and restored on one without it hits an
 *                        illegal instruction. generic runs the scalar equivalent on any CPU.
 *                        Command CPU -> "OK path=<vaes|generic|off> iters=N"
 *
 * State is SELF-VERIFYING: every page carries a header (magic, index,
 * generation, kind) and a body that is a pure function of (seed or instance
 * seed, index, generation, mode). `VERIFY` recomputes every page and file and
 * reports mismatches, so integrity after a migration is checked exactly, at
 * any time, under concurrent dirtying (single-threaded: no races).
 *
 * Protocol (line based, one command per line):
 *   PING    -> PONG
 *   VERIFY  -> OK bad=<n> pages=<N> gen=<sum of generations> files=<m> filebad=<k>
 *   STATS   -> OK pages=<N> dirtied=<n> filewrites=<n>
 *
 * Listening starts AFTER initialisation, so a TCP accept is the readiness point
 * (3.2). Build: gcc -O2 -static -o synth_workload synth_workload.c
 */
#define _GNU_SOURCE
#include <cpuid.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define PAGE 4096
#define HDR 32
#define MAGIC 0x53594e5448504147ULL /* "SYNTHPAG" */
#define FILE_SZ (256 * 1024)
#define MAXC 32

enum { KIND_SHARED = 0, KIND_INST = 1 };
enum { E_RANDOM = 0, E_ZERO = 1, E_REPEAT = 2 };

static uint64_t g_seed = 12345, g_inst_seed;
static int g_entropy = E_RANDOM;
static uint64_t g_pages, g_ws, g_nfiles;
static unsigned char *g_mem;
static uint64_t g_dirtied, g_filewrites;
static uint64_t *g_gen;     /* per-page generation (kept outside the pages) */
static uint64_t *g_fgen;    /* per-file generation */
static uint8_t *g_kind;     /* per-page kind */

static uint64_t mix(uint64_t x) { /* splitmix64 */
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static uint64_t page_seed(uint64_t idx, uint64_t gen, int kind) {
    return mix((kind == KIND_SHARED ? g_seed : g_inst_seed) ^ mix(idx * 0x100000001b3ULL + gen));
}

/* Fill the body of one page (everything after the header). */
static void fill_body(unsigned char *dst, uint64_t idx, uint64_t gen, int kind) {
    int mode = (kind == KIND_SHARED) ? E_RANDOM : g_entropy; /* shared pages are always deterministic noise */
    uint64_t s = page_seed(idx, gen, kind);
    uint64_t *w = (uint64_t *)dst;
    size_t n = (PAGE - HDR) / 8;
    if (mode == E_ZERO) { memset(dst, 0, PAGE - HDR); return; }
    if (mode == E_REPEAT) { uint64_t v = mix(s); for (size_t i = 0; i < n; i++) w[i] = v; return; }
    for (size_t i = 0; i < n; i++) { s = mix(s); w[i] = s; }
}

static void write_page(uint64_t idx) {
    unsigned char *p = g_mem + idx * PAGE;
    uint64_t *h = (uint64_t *)p;
    h[0] = MAGIC; h[1] = idx; h[2] = g_gen[idx]; h[3] = g_kind[idx];
    fill_body(p + HDR, idx, g_gen[idx], g_kind[idx]);
}

static int check_page(uint64_t idx) {
    unsigned char exp[PAGE] __attribute__((aligned(8)));
    unsigned char *p = g_mem + idx * PAGE;
    uint64_t *h = (uint64_t *)p;
    if (h[0] != MAGIC || h[1] != idx || h[2] != g_gen[idx] || h[3] != g_kind[idx]) return 1;
    fill_body(exp + HDR, idx, g_gen[idx], g_kind[idx]);
    return memcmp(p + HDR, exp + HDR, PAGE - HDR) != 0;
}

static void file_name(char *buf, size_t n, uint64_t k) { snprintf(buf, n, "/data/f%06llu.dat", (unsigned long long)k); }

static void file_content(unsigned char *buf, uint64_t k, uint64_t gen) {
    uint64_t s = mix(g_seed ^ mix(k * 0x9e37ULL + gen + 0xF11E));
    uint64_t *w = (uint64_t *)buf;
    for (size_t i = 0; i < FILE_SZ / 8; i++) { s = mix(s); w[i] = s; }
}

static int write_file(uint64_t k) {
    static unsigned char buf[FILE_SZ] __attribute__((aligned(8)));
    char path[64];
    file_name(path, sizeof path, k);
    file_content(buf, k, g_fgen[k]);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    size_t off = 0;
    while (off < FILE_SZ) { ssize_t r = write(fd, buf + off, FILE_SZ - off); if (r <= 0) { close(fd); return -1; } off += r; }
    close(fd);
    return 0;
}

static int check_file(uint64_t k) {
    static unsigned char buf[FILE_SZ] __attribute__((aligned(8))), exp[FILE_SZ] __attribute__((aligned(8)));
    char path[64];
    file_name(path, sizeof path, k);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 1;
    size_t off = 0;
    while (off < FILE_SZ) { ssize_t r = read(fd, buf + off, FILE_SZ - off); if (r <= 0) break; off += r; }
    close(fd);
    if (off != FILE_SZ) return 1;
    file_content(exp, k, g_fgen[k]);
    return memcmp(buf, exp, FILE_SZ) != 0;
}

static int g_cpu_mode;               /* 0 off, 1 vaes, 2 generic: chosen once at start */
static uint64_t g_cpu_iters;

static int cpu_has_vaes(void) {
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d) || !(c & (1u << 27)) || !(c & (1u << 28))) return 0;   /* OSXSAVE + AVX */
    if (__get_cpuid_max(0, 0) < 7) return 0;
    __cpuid_count(7, 0, a, b, c, d);
    return (c >> 9) & 1;                                                                        /* VAES */
}

static void cpu_step(void) {
    if (g_cpu_mode == 1) {
        __asm__ volatile("vpxor %%ymm0, %%ymm0, %%ymm0\n\tvpxor %%ymm1, %%ymm1, %%ymm1\n\t"
                         "vaesenc %%ymm1, %%ymm0, %%ymm0" ::: "ymm0", "ymm1");
    } else if (g_cpu_mode == 2) {
        volatile uint64_t x = g_cpu_iters * 0x9e3779b97f4a7c15ULL;
        (void)x;
    }
    if (g_cpu_mode) g_cpu_iters++;
}

static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }

static void reply(int fd, const char *s) { (void)!write(fd, s, strlen(s)); }

static void handle(int fd, const char *line) {
    char out[256];
    if (!strncmp(line, "PING", 4)) { reply(fd, "PONG\n"); return; }
    if (!strncmp(line, "VERIFY", 6)) {
        uint64_t bad = 0, gen = 0, fbad = 0;
        for (uint64_t i = 0; i < g_pages; i++) { bad += check_page(i); gen += g_gen[i]; }
        for (uint64_t k = 0; k < g_nfiles; k++) fbad += check_file(k);
        snprintf(out, sizeof out, "OK bad=%llu pages=%llu gen=%llu files=%llu filebad=%llu\n",
                 (unsigned long long)bad, (unsigned long long)g_pages, (unsigned long long)gen,
                 (unsigned long long)g_nfiles, (unsigned long long)fbad);
        reply(fd, out); return;
    }
    if (!strncmp(line, "CPU", 3)) {
        snprintf(out, sizeof out, "OK path=%s iters=%llu\n", g_cpu_mode == 1 ? "vaes" : g_cpu_mode == 2 ? "generic" : "off",
                 (unsigned long long)g_cpu_iters);
        reply(fd, out); return;
    }
    if (!strncmp(line, "STATS", 5)) {
        snprintf(out, sizeof out, "OK pages=%llu dirtied=%llu filewrites=%llu\n",
                 (unsigned long long)g_pages, (unsigned long long)g_dirtied, (unsigned long long)g_filewrites);
        reply(fd, out); return;
    }
    reply(fd, "ERR unknown command\n");
}

int main(int argc, char **argv) {
    uint64_t resident_mb = 64, ws = 0, file_mb = 0;
    double shared_frac = 1.0, dirty_pps = 0, files_pps = 0;
    int port = 9000;
    for (int i = 1; i + 1 < argc; i += 2) {
        const char *k = argv[i], *v = argv[i + 1];
        if (!strcmp(k, "--resident-mb")) resident_mb = strtoull(v, 0, 10);
        else if (!strcmp(k, "--shared-frac")) shared_frac = atof(v);
        else if (!strcmp(k, "--entropy")) g_entropy = !strcmp(v, "zero") ? E_ZERO : !strcmp(v, "repeat") ? E_REPEAT : E_RANDOM;
        else if (!strcmp(k, "--dirty-pps")) dirty_pps = atof(v);
        else if (!strcmp(k, "--ws-pages")) ws = strtoull(v, 0, 10);
        else if (!strcmp(k, "--file-mb")) file_mb = strtoull(v, 0, 10);
        else if (!strcmp(k, "--files-pps")) files_pps = atof(v);
        else if (!strcmp(k, "--seed")) g_seed = strtoull(v, 0, 10);
        else if (!strcmp(k, "--port")) port = atoi(v);
        else if (!strcmp(k, "--cpu-path")) g_cpu_mode = !strcmp(v, "off") ? 0 : !strcmp(v, "generic") ? 2 : (cpu_has_vaes() ? 1 : 2);
        else { fprintf(stderr, "unknown option %s\n", k); return 2; }
    }
    /* Instance-specific seed: differs between the template and the migrating
     * instance, so KIND_INST pages can never match the baseline. */
    int rfd = open("/dev/urandom", O_RDONLY);
    if (rfd < 0 || read(rfd, &g_inst_seed, sizeof g_inst_seed) != sizeof g_inst_seed) g_inst_seed = (uint64_t)now() ^ (uint64_t)getpid();
    if (rfd >= 0) close(rfd);

    g_pages = resident_mb * 1024 * 1024 / PAGE;
    if (g_pages == 0) g_pages = 1;
    g_ws = (ws && ws < g_pages) ? ws : g_pages;
    g_mem = mmap(NULL, g_pages * PAGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    g_gen = calloc(g_pages, sizeof *g_gen);
    g_kind = calloc(g_pages, 1);
    if (g_mem == MAP_FAILED || !g_gen || !g_kind) { fprintf(stderr, "alloc failed\n"); return 1; }
    uint64_t nshared = (uint64_t)(shared_frac * g_pages + 0.5);
    for (uint64_t i = 0; i < g_pages; i++) {
        /* Deterministic placement of the shared pages: the first nshared. */
        g_kind[i] = i < nshared ? KIND_SHARED : KIND_INST;
        write_page(i);
    }

    g_nfiles = file_mb * 1024 * 1024 / FILE_SZ;
    if (g_nfiles) {
        mkdir("/data", 0777);
        g_fgen = calloc(g_nfiles, sizeof *g_fgen);
        for (uint64_t k = 0; k < g_nfiles; k++) if (write_file(k) < 0) { fprintf(stderr, "file write failed\n"); return 1; }
    }

    int ls = socket(AF_INET, SOCK_STREAM, 0), one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(INADDR_ANY)};
    if (bind(ls, (struct sockaddr *)&a, sizeof a) < 0 || listen(ls, 16) < 0) { perror("bind/listen"); return 1; }
    fprintf(stderr, "synth_workload ready: %llu pages (%llu shared), ws %llu, %llu files, port %d\n",
            (unsigned long long)g_pages, (unsigned long long)nshared, (unsigned long long)g_ws,
            (unsigned long long)g_nfiles, port);

    struct { int fd; char buf[256]; int len; } cl[MAXC];
    for (int i = 0; i < MAXC; i++) cl[i].fd = -1;
    double t_last = now(), page_credit = 0, file_credit = 0;
    uint64_t cursor = 0, fcursor = 0;

    for (;;) {
        struct pollfd pf[MAXC + 1];
        int n = 0;
        pf[n].fd = ls; pf[n].events = POLLIN; n++;
        for (int i = 0; i < MAXC; i++) if (cl[i].fd >= 0) { pf[n].fd = cl[i].fd; pf[n].events = POLLIN; n++; }
        int busy = (dirty_pps > 0 || files_pps > 0);
        poll(pf, n, (busy || g_cpu_mode) ? 5 : 200);
        cpu_step();

        if (pf[0].revents & POLLIN) {
            int c = accept(ls, NULL, NULL);
            if (c >= 0) {
                int slot = -1;
                for (int i = 0; i < MAXC; i++) if (cl[i].fd < 0) { slot = i; break; }
                if (slot < 0) close(c); else { cl[slot].fd = c; cl[slot].len = 0; }
            }
        }
        for (int i = 0; i < MAXC; i++) {
            if (cl[i].fd < 0) continue;
            struct pollfd q = {cl[i].fd, POLLIN, 0};
            if (poll(&q, 1, 0) <= 0 || !(q.revents & (POLLIN | POLLHUP | POLLERR))) continue;
            ssize_t r = read(cl[i].fd, cl[i].buf + cl[i].len, sizeof cl[i].buf - 1 - cl[i].len);
            if (r <= 0) { close(cl[i].fd); cl[i].fd = -1; continue; }
            cl[i].len += r;
            cl[i].buf[cl[i].len] = 0;
            char *nl;
            while ((nl = strchr(cl[i].buf, '\n'))) {
                *nl = 0;
                handle(cl[i].fd, cl[i].buf);
                int rest = cl[i].len - (int)(nl + 1 - cl[i].buf);
                memmove(cl[i].buf, nl + 1, rest);
                cl[i].len = rest;
                cl[i].buf[rest] = 0;
            }
            if (cl[i].len >= (int)sizeof cl[i].buf - 1) cl[i].len = 0; /* drop an over-long line */
        }

        double t = now();
        double dt = t - t_last;
        t_last = t;
        if (dirty_pps > 0) {
            page_credit += dirty_pps * dt;
            while (page_credit >= 1.0) {
                uint64_t idx = (cursor++ * 2654435761ULL) % g_ws;
                g_gen[idx]++;
                write_page(idx);
                g_dirtied++;
                page_credit -= 1.0;
            }
        }
        if (files_pps > 0 && g_nfiles) {
            file_credit += files_pps * dt;
            while (file_credit >= 1.0) {
                uint64_t k = (fcursor++ * 40503ULL) % g_nfiles;
                g_fgen[k]++;
                write_file(k);
                g_filewrites++;
                file_credit -= 1.0;
            }
        }
    }
}
