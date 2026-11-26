"""bench/overhead.py: statistics helpers, a tiny real run, and the committed template."""

import importlib.util
import math
import re
import sys
from types import ModuleType

import pytest
import redis

from app.main import create_app
from tests.conftest import REPO_ROOT, make_settings, serve

BENCH = REPO_ROOT / "bench" / "overhead.py"


def load_bench() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bench_overhead", BENCH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look their module up here
    spec.loader.exec_module(module)
    return module


bench = load_bench()


def test_percentile() -> None:
    assert bench.percentile([1, 2, 3, 4, 5], 0.5) == 3
    assert bench.percentile([1, 2, 3, 4], 0.5) == 2.5
    assert bench.percentile([10], 0.99) == 10
    assert math.isnan(bench.percentile([], 0.5))


def test_histogram_quantile_matches_prometheus_rule() -> None:
    # 10 observations: 4 in (0, 0.005], 4 in (0.005, 0.01], 2 in (0.01, 0.025].
    buckets = [(0.005, 4.0), (0.01, 8.0), (0.025, 10.0), (math.inf, 10.0)]
    assert bench.histogram_quantile(0.5, buckets) == pytest.approx(0.00625)
    assert bench.histogram_quantile(0.9, buckets) == pytest.approx(0.0175)
    assert bench.histogram_quantile(0.4, buckets) == pytest.approx(0.005)
    assert math.isnan(bench.histogram_quantile(0.5, [(0.005, 0.0), (math.inf, 0.0)]))
    assert bench.histogram_quantile(0.99, [(0.005, 0.0), (math.inf, 3.0)]) == 0.005


def test_bucket_delta_and_parsing() -> None:
    text = "\n".join(
        [
            "# TYPE am_proxy_overhead_seconds histogram",
            'am_proxy_overhead_seconds_bucket{le="0.005",outcome="forwarded"} 7.0',
            'am_proxy_overhead_seconds_bucket{le="+Inf",outcome="forwarded"} 9.0',
            'am_proxy_overhead_seconds_bucket{le="0.005",outcome="refused"} 100.0',
            'am_proxy_overhead_seconds_bucket{le="+Inf",outcome="refused"} 100.0',
            'am_proxy_overhead_seconds_count{outcome="forwarded"} 9.0',
            'am_proxy_overhead_seconds_sum{outcome="forwarded"} 0.03',
        ]
    )
    after = bench.histogram_buckets(text)
    assert after == [(0.005, 7.0), (math.inf, 9.0)]  # refused calls are ignored
    assert bench.bucket_delta([(0.005, 2.0), (math.inf, 3.0)], after) == [
        (0.005, 5.0),
        (math.inf, 6.0),
    ]


def test_benchmark_runs_against_the_mock(redis_db: redis.Redis, tmp_path, live_mock) -> None:
    out = tmp_path / "results.md"
    with serve(create_app(make_settings(live_mock))) as proxy:
        code = bench.main(
            [
                "-n",
                "12",
                "-c",
                "3",
                "--warmup",
                "2",
                "--mock-url",
                live_mock,
                "--proxy-url",
                f"{proxy}/v1",
                "--metrics-url",
                f"{proxy}/metrics",
                "--out",
                str(out),
                "--notes",
                "pytest",
            ]
        )
    assert code == 0
    text = out.read_text(encoding="utf-8")
    assert "TBD" not in text
    assert re.search(r"\| Direct to mock \| 12 \| 0 \| [\d.]+ \| [\d.]+ \| [\d.]+ \|", text)
    assert re.search(r"\| Through AgentMeter \| 12 \| 0 \| [\d.]+ \| [\d.]+ \| [\d.]+ \|", text)
    # Server side: exactly the 12 proxied calls (warm-up excluded), forwarded outcome only.
    assert re.search(r"\n\| 12 \| [\d.]+ \| [\d.]+ \| [\d.]+ \| [\d.]+ \|\n", text)
    assert "| Requests per phase | 12 |" in text and "| Notes | pytest |" in text


def test_committed_results_are_placeholders_only() -> None:
    """No measured numbers may be committed: every value cell of the template is TBD."""
    text = (REPO_ROOT / "bench" / "results.md").read_text(encoding="utf-8")
    assert "TBD" in text
    rows = [line for line in text.splitlines() if line.startswith("| ") and "---" not in line]
    header_cells = {"Key", "Value", "Phase", "Observations"}
    for row in rows:
        cells = [c.strip() for c in row.strip("|").split("|")]
        if cells[0] in header_cells:
            continue
        values = cells[1:]
        assert all(v in ("TBD", "") for v in values), row
