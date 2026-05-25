"""
CPU and memory monitor for the benchmark.

Samples system-wide CPU% and the worker process tree's RSS every 0.5 s
in a background thread. Call snapshot() to read current averages/peaks.
"""

import contextlib
import threading
import time
from dataclasses import dataclass, field

import psutil


@dataclass
class Sample:
    cpu_pct: float
    rss_mb: float
    ts: float = field(default_factory=time.time)


class ResourceMonitor:
    """Lightweight background sampler for CPU and worker RSS."""

    def __init__(self, worker_pid: int | None = None, interval: float = 0.5) -> None:
        self._pid = worker_pid
        self._interval = interval
        self._samples: list[Sample] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    def _rss_mb(self, proc: psutil.Process) -> float:
        """Sum RSS for the process and all its children (worker tree)."""
        try:
            total = proc.memory_info().rss
            for child in proc.children(recursive=True):
                with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                    total += child.memory_info().rss
            return total / 1_048_576
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

    def _loop(self) -> None:
        proc = psutil.Process(self._pid) if self._pid else None
        while not self._stop.is_set():
            cpu = psutil.cpu_percent(interval=None)
            rss = self._rss_mb(proc) if proc else 0.0
            with self._lock:
                self._samples.append(Sample(cpu_pct=cpu, rss_mb=rss))
            time.sleep(self._interval)

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            if not self._samples:
                return {
                    "cpu_avg": 0.0,
                    "cpu_peak": 0.0,
                    "rss_avg_mb": 0.0,
                    "rss_peak_mb": 0.0,
                    "rss_mb": 0.0,
                }
            cpus = [s.cpu_pct for s in self._samples]
            rss = [s.rss_mb for s in self._samples]
            return {
                "cpu_avg": round(sum(cpus) / len(cpus), 1),
                "cpu_peak": round(max(cpus), 1),
                "rss_avg_mb": round(sum(rss) / len(rss), 1),
                "rss_peak_mb": round(max(rss), 1),
                "rss_mb": round(rss[-1], 1),  # last sample
            }

    def reset(self) -> None:
        with self._lock:
            self._samples.clear()
