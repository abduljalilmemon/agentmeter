"""Prometheus metrics (PLAN §6, M5). Each proxy app and each worker owns a registry.

Floats appear only here, at the export edge: budget logic is integer micro-USD and
converts with ``micro / 1e6`` at the moment a counter is incremented.

Labels are bounded: ``agent`` and ``team`` (configured names, or "" when the header is
absent), ``reason``, ``level`` and ``status``. Never ``run_id``: runs are unbounded and
would blow up the series count. Per-run numbers are in ``GET /runs/{id}``.

Proxy (GET /metrics on every replica):

    am_spend_usd_total{agent,team}          USD settled (actual cost), by agent and team
    am_breaker_trips_total{reason}          breaker openings: loop | step_cap (once per
                                            opening, not per refused call)
    am_proxy_overhead_seconds{outcome}      time in the proxy, upstream excluded (below);
                                            outcome = forwarded | refused
    am_settle_failures_total                settles that failed (cost not recorded)
    am_requests_total{status,reason,level}  /v1/chat/completions responses; reason and
                                            level are set for 429s (budget/run, loop, ...)

Worker (its own port, ``worker_metrics_port``):

    am_alerts_delivered_total               webhook posts answered 2xx
    am_alerts_dead_total                    alerts dead-lettered after max_attempts

``am_proxy_overhead_seconds``: wall time from the route handler's start until the proxy
has a response to return, minus the time spent waiting for the upstream. For a plain
call the upstream time is the whole upstream request (send to last body byte); for a
stream it is the time to the upstream's response headers (``client.send``), and the
relay of the stream itself is not counted. ``outcome="forwarded"``: the proxy sent the
call upstream (whatever the upstream then answered, including connection failures).
``outcome="refused"``: answered without contacting the upstream (400, 429, 503), so
the whole handler time is overhead. The settle after a stream has ended is not included
(the client already has every byte by then).
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Histogram

MICRO_PER_USD_F = 1e6  # export edge only

# Overhead is milliseconds-scale; buckets from 0.5 ms to 2.5 s.
OVERHEAD_BUCKETS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
)


class ProxyMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.spend = Counter(
            "am_spend_usd", "USD settled (actual cost)", ["agent", "team"], registry=r
        )
        self.breaker_trips = Counter(
            "am_breaker_trips", "Circuit breaker openings", ["reason"], registry=r
        )
        self.overhead = Histogram(
            "am_proxy_overhead_seconds",
            "Time spent in the proxy, upstream time excluded",
            ["outcome"],
            buckets=OVERHEAD_BUCKETS,
            registry=r,
        )
        self.settle_failures = Counter(
            "am_settle_failures", "Settles that failed; their cost was not recorded", registry=r
        )
        self.requests = Counter(
            "am_requests",
            "Chat completion responses by status (429s by reason and level)",
            ["status", "reason", "level"],
            registry=r,
        )

    def record_spend(self, agent: str | None, team: str | None, micro: int) -> None:
        if micro > 0:
            self.spend.labels(agent or "", team or "").inc(micro / MICRO_PER_USD_F)

    def record_trip(self, reason: str) -> None:
        self.breaker_trips.labels(reason).inc()


class WorkerMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.delivered = Counter(
            "am_alerts_delivered", "Alerts posted to the webhook (2xx)", registry=self.registry
        )
        self.dead = Counter(
            "am_alerts_dead", "Alerts dead-lettered after max_attempts", registry=self.registry
        )
