"""Small-sample statistics for the evaluation. Pure stdlib.

Rules this module enforces for every reported cell:
  * dispersion is the SAMPLE standard deviation (n-1), never the population SD;
  * location estimates come with a seeded percentile-bootstrap confidence
    interval, so a figure can show uncertainty instead of a bare mean;
  * comparisons report an effect size (Cliff's delta + ratio of means with a
    bootstrap CI), not a significance claim -- at n=5..10 per cell a p-value
    would overstate what the data can say.

Usage:
    from stats import summarize, compare
    summarize([4.1, 4.3, 4.2, 4.6, 4.0])   -> dict(n, mean, sd, median, ci_mean, ci_median)
    compare(a, b)                          -> dict(ratio_of_means, ci_ratio, cliffs_delta)
"""
import math
import random
import statistics

N_BOOT = 10000
SEED = 20261002  # fixed: reruns of an analysis give identical intervals


def sample_sd(xs):
    """Sample SD (ddof=1); 0.0 for n < 2 so tables stay printable."""
    return statistics.stdev(xs) if len(xs) > 1 else 0.0


def _percentile(sorted_xs, q):
    if not sorted_xs:
        return float("nan")
    k = (len(sorted_xs) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return sorted_xs[lo] + (sorted_xs[hi] - sorted_xs[lo]) * (k - lo)


def bootstrap_ci(xs, stat=statistics.mean, conf=0.95, n_boot=N_BOOT, seed=SEED):
    """Percentile bootstrap CI of `stat` over xs. With n < 2 returns (x, x)."""
    xs = [float(x) for x in xs]
    if len(xs) < 2:
        v = stat(xs) if xs else float("nan")
        return (v, v)
    rng = random.Random(seed)
    n = len(xs)
    boots = sorted(stat([xs[rng.randrange(n)] for _ in range(n)]) for _ in range(n_boot))
    a = (1 - conf) / 2
    return (_percentile(boots, a), _percentile(boots, 1 - a))


def summarize(xs, conf=0.95):
    xs = [float(x) for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    return {
        "n": len(xs),
        "mean": statistics.mean(xs),
        "sd": sample_sd(xs),
        "median": statistics.median(xs),
        "min": min(xs),
        "max": max(xs),
        "ci_mean": bootstrap_ci(xs, statistics.mean, conf),
        "ci_median": bootstrap_ci(xs, statistics.median, conf),
    }


def cliffs_delta(a, b):
    """P(a > b) - P(a < b): -1..1, 0 = full overlap. Distribution-free."""
    if not a or not b:
        return float("nan")
    gt = sum(1 for x in a for y in b if x > y)
    lt = sum(1 for x in a for y in b if x < y)
    return (gt - lt) / (len(a) * len(b))


def compare(a, b, conf=0.95, n_boot=N_BOOT, seed=SEED):
    """How much larger is a than b? Ratio of means with a bootstrap CI (both
    groups resampled independently) plus Cliff's delta."""
    a = [float(x) for x in a]
    b = [float(x) for x in b]
    if len(a) < 2 or len(b) < 2 or statistics.mean(b) == 0:
        return {"ratio_of_means": float("nan"), "ci_ratio": (float("nan"),) * 2,
                "cliffs_delta": cliffs_delta(a, b)}
    rng = random.Random(seed)
    na, nb = len(a), len(b)
    ratios = []
    for _ in range(n_boot):
        mb = statistics.mean([b[rng.randrange(nb)] for _ in range(nb)])
        if mb == 0:
            continue
        ratios.append(statistics.mean([a[rng.randrange(na)] for _ in range(na)]) / mb)
    ratios.sort()
    al = (1 - conf) / 2
    return {
        "ratio_of_means": statistics.mean(a) / statistics.mean(b),
        "ci_ratio": (_percentile(ratios, al), _percentile(ratios, 1 - al)),
        "cliffs_delta": cliffs_delta(a, b),
    }


def fmt(s, unit=""):
    """'4.21 +/- 0.27 (95% CI 4.0-4.5), n=5' for tables."""
    if not s.get("n"):
        return "n=0"
    lo, hi = s["ci_mean"]
    return f"{s['mean']:.2f}{unit} +/- {s['sd']:.2f} (95% CI {lo:.2f}-{hi:.2f}), n={s['n']}"


if __name__ == "__main__":
    # Self-check against hand-computed values.
    xs = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    assert abs(sample_sd(xs) - 2.13809) < 1e-4, sample_sd(xs)          # n-1 form; population SD would be 2.0
    assert cliffs_delta([5, 6, 7], [1, 2, 3]) == 1.0
    assert cliffs_delta([1, 2, 3], [1, 2, 3]) == 0.0
    s = summarize(xs)
    assert s["ci_mean"][0] < s["mean"] < s["ci_mean"][1]
    assert summarize(xs)["ci_mean"] == summarize(xs)["ci_mean"]          # deterministic
    c = compare([10, 11, 12], [5, 5.5, 6])
    assert 1.8 < c["ratio_of_means"] < 2.2 and c["cliffs_delta"] == 1.0
    print("stats.py self-check OK;", fmt(s))
