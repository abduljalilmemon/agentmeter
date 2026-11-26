"""Proxy overhead benchmark: direct-to-mock vs through AgentMeter.

Runs N chat completions at concurrency C twice against the same mock upstream:

1. straight to mock_upstream (the baseline), and
2. through the proxy, each call with its own run id (so no budget, loop or step-cap
   refusals get in the way: every call is forwarded).

It reports client-side latency percentiles for both, their difference, and the proxy's
own measurement: the change in ``am_proxy_overhead_seconds{outcome="forwarded"}`` over
the proxied phase, read from /metrics before and after (percentiles interpolated from
the histogram buckets, like Prometheus' ``histogram_quantile``). Results go to a
markdown file with the machine and environment details.

    docker compose up -d --build            # proxy :8080, mock :8001, redis
    python bench/overhead.py -n 2000 -c 20  # writes bench/results.md

Nothing here calls a real LLM API.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import platform
import subprocess
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families

OVERHEAD = "am_proxy_overhead_seconds"
BODY = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 32}
QUANTILES = (0.50, 0.95, 0.99)


# --- statistics -----------------------------------------------------------------------


def percentile(values: Sequence[float], q: float) -> float:
    """Linear interpolation between closest ranks (numpy's default method)."""
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = q * (len(ordered) - 1)
    low, high = math.floor(rank), math.ceil(rank)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


Buckets = list[tuple[float, float]]  # (upper bound "le", cumulative count), ascending


def histogram_buckets(metrics_text: str, outcome: str = "forwarded") -> Buckets:
    buckets: Buckets = []
    for family in text_string_to_metric_families(metrics_text):
        if family.name != OVERHEAD:
            continue
        for sample in family.samples:
            if sample.name == f"{OVERHEAD}_bucket" and sample.labels.get("outcome") == outcome:
                buckets.append((float(sample.labels["le"]), sample.value))
    return sorted(buckets)


def bucket_delta(before: Buckets, after: Buckets) -> Buckets:
    previous = dict(before)
    return [(le, count - previous.get(le, 0.0)) for le, count in after]


def histogram_quantile(q: float, buckets: Buckets) -> float:
    """Prometheus' histogram_quantile on cumulative buckets (linear within a bucket)."""
    if not buckets or buckets[-1][1] <= 0:
        return math.nan
    total = buckets[-1][1]
    rank = q * total
    lower_bound, lower_count = 0.0, 0.0
    for le, count in buckets:
        if count >= rank:
            if math.isinf(le):
                return lower_bound  # in the +Inf bucket: the highest finite bound
            if count == lower_count:
                return le
            return lower_bound + (le - lower_bound) * (rank - lower_count) / (count - lower_count)
        lower_bound, lower_count = le, count
    return lower_bound


# --- load -----------------------------------------------------------------------------


@dataclass
class Phase:
    name: str
    latencies: list[float]  # seconds, successful calls only
    errors: int
    wall_seconds: float

    def q(self, quantile: float) -> float:
        return percentile(self.latencies, quantile)


async def run_phase(
    name: str, url: str, n: int, concurrency: int, proxied: bool, warmup: int
) -> Phase:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=60, limits=limits) as http:
        sem = asyncio.Semaphore(concurrency)
        latencies: list[float] = []
        errors = 0

        async def one(record: bool) -> None:
            nonlocal errors
            headers = {"Authorization": "Bearer sk-bench"}
            if proxied:
                headers["X-AM-Run-Id"] = f"bench-{uuid.uuid4().hex}"
            async with sem:
                started = time.perf_counter()
                try:
                    resp = await http.post(f"{url}/chat/completions", json=BODY, headers=headers)
                    ok = resp.status_code == 200
                except httpx.HTTPError:
                    ok = False
                elapsed = time.perf_counter() - started
            if not record:
                return
            if ok:
                latencies.append(elapsed)
            else:
                errors += 1

        await asyncio.gather(*(one(False) for _ in range(warmup)))  # connections, caches
        started = time.perf_counter()
        await asyncio.gather(*(one(True) for _ in range(n)))
        return Phase(name, latencies, errors, time.perf_counter() - started)


# --- report ---------------------------------------------------------------------------


def _git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5
        )
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def environment(args: argparse.Namespace) -> list[tuple[str, str]]:
    return [
        ("Date (UTC)", datetime.now(UTC).strftime("%Y-%m-%d %H:%M")),
        ("Git commit", _git_commit()),
        ("OS", platform.platform()),
        ("CPU", platform.processor() or platform.machine()),
        ("CPU count", str(os.cpu_count())),
        ("Python (client)", platform.python_version()),
        ("Requests per phase", str(args.requests)),
        ("Concurrency", str(args.concurrency)),
        ("Warm-up calls", str(args.warmup)),
        ("Mock URL", args.mock_url),
        ("Proxy URL", args.proxy_url),
        ("Notes", args.notes or "-"),
    ]


def _ms(seconds: float) -> str:
    return "n/a" if math.isnan(seconds) else f"{seconds * 1000:.2f}"


def render(
    args: argparse.Namespace,
    direct: Phase,
    proxied: Phase,
    server: Buckets,
    server_sum: float,
) -> str:
    server_count = server[-1][1] if server else 0.0
    lines = [
        "# Proxy overhead benchmark",
        "",
        "Generated by `bench/overhead.py`. Both phases call the same mock upstream; the",
        "difference between them is what the proxy adds (budget reserve/settle, breaker",
        "gate, event writes, HTTP hop). Latencies in milliseconds.",
        "",
        "## Environment",
        "",
        "| Key | Value |",
        "|---|---|",
        *(f"| {k} | {v} |" for k, v in environment(args)),
        "",
        "## Client-side latency",
        "",
        "| Phase | Calls OK | Errors | p50 | p95 | p99 | Throughput (calls/s) |",
        "|---|---|---|---|---|---|---|",
    ]
    for phase in (direct, proxied):
        rate = len(phase.latencies) / phase.wall_seconds if phase.wall_seconds else math.nan
        lines.append(
            f"| {phase.name} | {len(phase.latencies)} | {phase.errors} | "
            f"{_ms(phase.q(0.50))} | {_ms(phase.q(0.95))} | {_ms(phase.q(0.99))} | "
            f"{rate:.1f} |"
        )
    lines.append(
        "| Difference (proxy - direct) | | | "
        + " | ".join(_ms(proxied.q(q) - direct.q(q)) for q in QUANTILES)
        + " | |"
    )
    mean = server_sum / server_count if server_count else math.nan
    lines += [
        "",
        '## Server-side overhead (`am_proxy_overhead_seconds{outcome="forwarded"}`)',
        "",
        "Measured inside the proxy over the proxied phase (warm-up excluded): handler time",
        "minus upstream time. Percentiles are interpolated from the histogram buckets, so",
        "they are only as fine as the buckets (0.5 ms, 1 ms, 2.5 ms, 5 ms, 10 ms, ...).",
        "",
        "| Observations | Mean | p50 | p95 | p99 |",
        "|---|---|---|---|---|",
        f"| {int(server_count)} | {_ms(mean)} | "
        + " | ".join(_ms(histogram_quantile(q, server)) for q in QUANTILES)
        + " |",
        "",
    ]
    return "\n".join(lines)


async def scrape(metrics_url: str) -> tuple[Buckets, float]:
    async with httpx.AsyncClient(timeout=10) as http:
        text = (await http.get(metrics_url)).text
    total = 0.0
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name == f"{OVERHEAD}_sum" and sample.labels.get("outcome") == "forwarded":
                total = sample.value
    return histogram_buckets(text), total


async def benchmark(args: argparse.Namespace) -> str:
    direct = await run_phase(
        "Direct to mock", args.mock_url, args.requests, args.concurrency, False, args.warmup
    )
    # Warm the proxy before the "before" scrape so the warm-up isn't in the delta.
    await run_phase("warm-up", args.proxy_url, 0, args.concurrency, True, args.warmup)
    before, before_sum = await scrape(args.metrics_url)
    proxied = await run_phase(
        "Through AgentMeter", args.proxy_url, args.requests, args.concurrency, True, 0
    )
    after, after_sum = await scrape(args.metrics_url)
    return render(args, direct, proxied, bucket_delta(before, after), after_sum - before_sum)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AgentMeter proxy overhead benchmark")
    p.add_argument("-n", "--requests", type=int, default=1000, help="calls per phase")
    p.add_argument("-c", "--concurrency", type=int, default=10)
    p.add_argument("--warmup", type=int, default=50, help="unrecorded calls before each phase")
    p.add_argument("--mock-url", default="http://localhost:8001/v1")
    p.add_argument("--proxy-url", default="http://localhost:8080/v1")
    p.add_argument("--metrics-url", default="http://localhost:8080/metrics")
    p.add_argument("--out", type=Path, default=Path(__file__).with_name("results.md"))
    p.add_argument("--notes", default="", help="free text for the environment table")
    args = p.parse_args(argv)
    if args.requests < 1 or args.concurrency < 1 or args.warmup < 0:
        p.error("--requests and --concurrency must be >= 1, --warmup >= 0")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = asyncio.run(benchmark(args))
    args.out.write_text(report, encoding="utf-8")
    print(report)
    print(f"written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
