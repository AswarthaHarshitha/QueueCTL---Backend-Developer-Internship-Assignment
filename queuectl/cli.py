import argparse
import json
import os
import sys
import time
import subprocess
from datetime import datetime

from . import db
from . import worker as worker_mod
from . import metrics as metrics_mod


PID_FILE = os.path.expanduser("~/.queuectl/pid")


def _write_pid(pid: int):
    d = os.path.dirname(PID_FILE)
    os.makedirs(d, exist_ok=True)
    with open(PID_FILE, "w") as f:
        f.write(str(pid))


def _read_pid():
    try:
        with open(PID_FILE, "r") as f:
            return int(f.read().strip())
    except Exception:
        return None


def enqueue(args):
    data = args.json
    # ensure DB and tables exist
    db.init_db()
    try:
        job = json.loads(data)
    except Exception:
        print("Invalid JSON")
        return 2
    if "id" not in job or "command" not in job:
        print("Job must include id and command")
        return 2
    job.setdefault("state", "pending")
    job.setdefault("attempts", 0)
    if "max_retries" not in job:
        cfg = db.get_config("default-max-retries")
        job["max_retries"] = int(cfg) if cfg else 3
    # accept priority if provided
    job.setdefault("priority", job.get("priority", 0))
    # accept tags and run_at scheduling
    if "tags" in job and isinstance(job["tags"], list):
        job["tags"] = ",".join(job["tags"])
    job.setdefault("run_at", job.get("run_at"))
    now = datetime.utcnow().isoformat() + "Z"
    job.setdefault("created_at", now)
    job.setdefault("updated_at", now)
    try:
        db.insert_job(job)
        print("Enqueued", job["id"])
        return 0
    except Exception as e:
        print("Failed to enqueue:", e)
        return 1


def worker_start(args):
    count = args.count
    daemon = args.daemon
    base = int(db.get_config("backoff-base") or 2)
    if daemon:
        # spawn background process
        cmd = [sys.executable, "-m", "queuectl", "run-daemon", "--count", str(count)]
        d = os.path.expanduser("~/.queuectl")
        os.makedirs(d, exist_ok=True)
        out = open(os.path.join(d, "daemon.out"), "a")
        err = open(os.path.join(d, "daemon.err"), "a")
        p = subprocess.Popen(cmd, stdout=out, stderr=err, stdin=subprocess.DEVNULL)
        _write_pid(p.pid)
        print("Started daemon pid", p.pid)
        print("Daemon logs:", os.path.join(d, "daemon.out"), os.path.join(d, "daemon.err"))
        return 0
    else:
        print("Starting", count, "workers (foreground). Ctrl+C to stop")
        db.init_db()
        worker_mod.start_workers(count, base)
        return 0


def worker_stop(args):
    pid = _read_pid()
    if not pid:
        print("No daemon PID file found")
        return 1
    try:
        os.kill(pid, 15)
        print("Sent SIGTERM to pid", pid)
        try:
            os.remove(PID_FILE)
        except Exception:
            pass
        return 0
    except Exception as e:
        print("Failed to stop daemon:", e)
        return 1


def status(args):
    db.init_db()
    counts = db.job_counts()
    print("Jobs:")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    pid = _read_pid()
    if pid:
        print("Daemon PID:", pid)
    else:
        print("No daemon running")
    return 0


def list_cmd(args):
    db.init_db()
    items = db.list_jobs(args.state)
    for j in items:
        print(json.dumps(j))
    return 0


def dlq_list(args):
    db.init_db()
    items = db.list_jobs("dead")
    for j in items:
        print(json.dumps(j))
    return 0


def dlq_retry(args):
    db.init_db()
    job = db.get_job(args.job_id)
    if not job:
        print("No such job")
        return 1
    if job["state"] != "dead":
        print("Job not in DLQ")
        return 2
    db.move_dlq_to_pending(args.job_id)
    print("Moved to pending:", args.job_id)
    return 0


def config_set(args):
    db.init_db()
    db.set_config(args.key, args.value)
    print("Set", args.key, args.value)
    return 0


def run_daemon(args):
    # runs master process that spawns worker processes
    count = args.count
    base = int(db.get_config("backoff-base") or 2)
    db.init_db()
    # write own pid
    _write_pid(os.getpid())
    print("Daemon running pid", os.getpid())
    try:
        worker_mod.start_workers(count, base)
    finally:
        try:
            os.remove(PID_FILE)
        except Exception:
            pass


def build_parser():
    p = argparse.ArgumentParser(prog="queuectl")
    sub = p.add_subparsers(dest="cmd")

    enq = sub.add_parser("enqueue")
    enq.add_argument("json")
    enq.set_defaults(func=enqueue)

    w = sub.add_parser("worker")
    wsub = w.add_subparsers(dest="subcmd")
    ws = wsub.add_parser("start")
    ws.add_argument("--count", type=int, default=1)
    ws.add_argument("--daemon", action="store_true")
    ws.set_defaults(func=worker_start)
    wstop = wsub.add_parser("stop")
    wstop.set_defaults(func=worker_stop)

    sub.add_parser("status").set_defaults(func=status)

    lst = sub.add_parser("list")
    lst.add_argument("--state", default=None)
    lst.set_defaults(func=list_cmd)

    dlq = sub.add_parser("dlq")
    dlqsub = dlq.add_subparsers(dest="subcmd")
    dlqsub.add_parser("list").set_defaults(func=dlq_list)
    r = dlqsub.add_parser("retry")
    r.add_argument("job_id")
    r.set_defaults(func=dlq_retry)

    cfg = sub.add_parser("config")
    cfgsub = cfg.add_subparsers(dest="subcmd")
    cs = cfgsub.add_parser("set")
    cs.add_argument("key")
    cs.add_argument("value")
    cs.set_defaults(func=config_set)

    # internal
    rd = sub.add_parser("run-daemon")
    rd.add_argument("--count", type=int, default=1)
    rd.set_defaults(func=run_daemon)
    metrics = sub.add_parser("metrics")
    metrics_sub = metrics.add_subparsers(dest="subcmd")
    ms = metrics_sub.add_parser("serve")
    ms.add_argument("--port", type=int, default=8000)
    ms.set_defaults(func=lambda a: metrics_mod.serve(a.port))

    return p


def main(argv=None):
    p = build_parser()
    args = p.parse_args(argv)
    if not hasattr(args, "func"):
        p.print_help()
        return 2
    return args.func(args)
