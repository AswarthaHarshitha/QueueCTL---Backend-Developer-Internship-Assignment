import os
import signal
import time
import subprocess
import multiprocessing as mp
from datetime import datetime
from typing import Optional

from . import db

import pathlib


def _logs_dir() -> str:
    d = os.path.expanduser("~/.queuectl/logs")
    pathlib.Path(d).mkdir(parents=True, exist_ok=True)
    return d


TERMINATE = mp.Event()


def worker_loop(worker_name: str, base_backoff: int):
    """Single worker loop: pick, run, update"""
    while not TERMINATE.is_set():
        now_iso = datetime.utcnow().isoformat() + "Z"
        job = db.fetch_and_lock_job(worker_name, now_iso)
        if not job:
            time.sleep(1)
            continue

        job_id = job["id"]
        cmd = job["command"]
        attempts = job.get("attempts", 0) + 1
        max_retries = job.get("max_retries") or db.get_config("default-max-retries")
        if max_retries is not None:
            try:
                max_retries = int(max_retries)
            except Exception:
                max_retries = 3

        # job timeout from config
        timeout = int(db.get_config("job-timeout") or 10)
        logfile = None
        try:
            logs_dir = _logs_dir()
            logfile = os.path.join(logs_dir, f"{job_id}.log")
            p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            output = (p.stdout or "") + (p.stderr or "")
            # write to logfile
            with open(logfile, "w") as f:
                f.write(output)
            if p.returncode == 0:
                db.complete_job(job_id, output)
            else:
                db.fail_job(job_id, attempts, max_retries, base_backoff, output)
        except subprocess.TimeoutExpired as e:
            msg = f"Job timed out after {timeout}s\n"
            # Try to capture partial output if any
            out = (getattr(e, 'output', '') or '') + (getattr(e, 'stderr', '') or '')
            full = msg + out
            if logfile:
                with open(logfile, "w") as f:
                    f.write(full)
            db.fail_job(job_id, attempts, max_retries, base_backoff, full)
        except FileNotFoundError as e:
            db.fail_job(job_id, attempts, max_retries, base_backoff, str(e))
        except Exception as e:
            db.fail_job(job_id, attempts, max_retries, base_backoff, str(e))


def _run_worker_process(worker_id: int, base_backoff: int):
    name = f"worker-{os.getpid()}-{worker_id}"
    try:
        worker_loop(name, base_backoff)
    except KeyboardInterrupt:
        # Graceful
        return


def start_workers(count: int, base_backoff: int):
    procs = []

    for i in range(count):
        p = mp.Process(target=_run_worker_process, args=(i, base_backoff))
        p.start()
        procs.append(p)

    def _signal_handler(sig, frame):
        TERMINATE.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Wait for children to exit
    try:
        while any(p.is_alive() for p in procs):
            time.sleep(0.5)
    finally:
        TERMINATE.set()
        for p in procs:
            p.join(timeout=5)
