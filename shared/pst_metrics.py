"""Prometheus metrics for PST export observability.

Initialised lazily so processes that never start a PST job don't pay the
import cost.  All metrics are no-ops if ``prometheus_client`` is unavailable
(e.g. local-dev pods without the dep installed).

Scrape with:
    docker compose exec restore-worker curl -s http://localhost:9100/metrics

Mount via the prometheus container's scrape_config:
    - job_name: pst_export
      static_configs:
        - targets:
            - restore-worker-1:9100
            - restore-worker-2:9100
            - restore-worker-heavy:9100
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# Single source of truth for whether the metrics layer is active.
_ENABLED = False
_HTTP_STARTED = False
_LOCK = threading.Lock()

# Lazy-initialised metrics — None when prometheus_client missing.
job_total = None              # Counter[status, granularity]
items_total = None            # Counter[item_type]
items_failed_total = None     # Counter[item_type, reason]
job_duration_seconds = None   # Histogram[granularity, status]
group_duration_seconds = None # Histogram[item_type]
attachment_skipped_total = None  # Counter (oversize cap)
worker_rss_mb = None          # Gauge — sampled snapshot
queue_active_jobs = None      # Gauge[scope_key, scope]


def init(metrics_port: Optional[int] = None) -> bool:
    """Bring up the metrics HTTP server (idempotent). Returns True when
    metrics are active. Call once from the worker entry point.
    """
    global _ENABLED, _HTTP_STARTED, job_total, items_total, items_failed_total
    global job_duration_seconds, group_duration_seconds, attachment_skipped_total
    global worker_rss_mb, queue_active_jobs

    with _LOCK:
        if _HTTP_STARTED:
            return _ENABLED
        try:
            from prometheus_client import (
                Counter, Histogram, Gauge, start_http_server,
            )
        except Exception as exc:
            logger.warning("[pst_metrics] prometheus_client unavailable (%s) — metrics disabled", exc)
            _HTTP_STARTED = True
            return False

        # Counters
        job_total = Counter(
            "pst_export_jobs_total",
            "PST export jobs by terminal status",
            ["status", "granularity"],
        )
        items_total = Counter(
            "pst_export_items_total",
            "Items written to PST archives",
            ["item_type"],
        )
        items_failed_total = Counter(
            "pst_export_items_failed_total",
            "Items that failed to write (after retries)",
            ["item_type", "reason"],
        )
        attachment_skipped_total = Counter(
            "pst_export_attachments_skipped_total",
            "Attachments replaced with placeholder (>PST_MAX_ATTACHMENT_BYTES)",
        )
        # Histograms — bucket layout tuned for export durations
        # (most under 1m, 99p around 1h, whales out to 4h+).
        job_duration_seconds = Histogram(
            "pst_export_job_duration_seconds",
            "Wall-clock duration of a PST job (apply_license to final blob upload)",
            ["granularity", "status"],
            buckets=(5, 10, 30, 60, 300, 900, 1800, 3600, 7200, 14400),
        )
        group_duration_seconds = Histogram(
            "pst_export_group_duration_seconds",
            "Per-group write+upload duration",
            ["item_type"],
            buckets=(0.5, 1, 5, 10, 30, 60, 300, 900, 1800),
        )
        # Gauges
        worker_rss_mb = Gauge(
            "pst_export_worker_rss_mb",
            "Current RSS of the worker process (sampled when memory check fires)",
        )
        queue_active_jobs = Gauge(
            "pst_export_active_jobs",
            "Active PST jobs per rate-limit scope key (drained from Redis)",
            ["scope", "scope_key"],
        )

        port = metrics_port if metrics_port is not None else int(
            os.environ.get("PST_METRICS_PORT", "9100")
        )
        try:
            start_http_server(port)
            logger.info("[pst_metrics] HTTP server listening on :%d", port)
            _ENABLED = True
        except OSError as exc:
            logger.warning("[pst_metrics] could not start HTTP server on :%d (%s) — metrics disabled", port, exc)
            _ENABLED = False
        _HTTP_STARTED = True
        return _ENABLED


def safe_inc(metric, *labels, amount: float = 1.0) -> None:
    """No-op when metrics are disabled — keeps callers from sprinkling
    ``if metric is not None`` everywhere."""
    if metric is None:
        return
    try:
        if labels:
            metric.labels(*labels).inc(amount)
        else:
            metric.inc(amount)
    except Exception as exc:
        logger.debug("metric inc failed: %s", exc)


def safe_observe(metric, value: float, *labels) -> None:
    if metric is None:
        return
    try:
        if labels:
            metric.labels(*labels).observe(value)
        else:
            metric.observe(value)
    except Exception as exc:
        logger.debug("metric observe failed: %s", exc)


def safe_set(metric, value: float, *labels) -> None:
    if metric is None:
        return
    try:
        if labels:
            metric.labels(*labels).set(value)
        else:
            metric.set(value)
    except Exception as exc:
        logger.debug("metric set failed: %s", exc)
