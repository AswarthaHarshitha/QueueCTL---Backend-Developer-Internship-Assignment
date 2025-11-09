import os
import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, Any


def _db_path() -> str:
    d = os.path.expanduser("~/.queuectl")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "queue.db")


def get_conn():
    path = _db_path()
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        next_run_at TEXT,
        locked_by TEXT,
        locked_at TEXT,
        output TEXT
    )
    """)

    # Add optional columns for newer features (priority, output_file)
    # Use ALTER TABLE ADD COLUMN which is safe if the column exists will raise; ignore errors
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN output_file TEXT")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN tags TEXT")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN run_at TEXT")
    except Exception:
        pass

    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    # default config values
    defaults = {
        "default-max-retries": "3",
        "backoff-base": "2",
        "job-timeout": "10"
    }
    for k, v in defaults.items():
        cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES(?,?)", (k, v))

    conn.commit()
    return conn


def insert_job(job: Dict[str, Any]):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    cur.execute(
        "INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at,next_run_at,priority,output_file) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            job["id"],
            job["command"],
            job.get("state", "pending"),
            job.get("attempts", 0),
            job.get("max_retries"),
            job.get("created_at", now),
            job.get("updated_at", now),
            job.get("next_run_at"),
            job.get("priority", 0),
            job.get("output_file"),
        ),
    )
    conn.commit()


def get_config(key: str) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def set_config(key: str, value: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (key, value))
    conn.commit()


def list_jobs(state: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY priority DESC, created_at", (state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY priority DESC, created_at")
    return [dict(r) for r in cur.fetchall()]


def job_counts():
    conn = get_conn()
    cur = conn.cursor()
    states = ["pending", "processing", "completed", "failed", "dead"]
    res = {}
    for s in states:
        cur.execute("SELECT COUNT(1) FROM jobs WHERE state=?", (s,))
        res[s] = cur.fetchone()[0]
    return res


def fetch_and_lock_job(worker_id: str, now_iso: str):
    """Atomically pick one eligible job and set it to processing. Returns job dict or None.

    Eligible: state in ('pending','failed') and (next_run_at IS NULL OR next_run_at <= now)
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            "SELECT id FROM jobs WHERE (state='pending' OR state='failed') AND (next_run_at IS NULL OR next_run_at<=?) AND (run_at IS NULL OR run_at<=?) ORDER BY priority DESC, created_at LIMIT 1",
            (now_iso, now_iso),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        job_id = row[0]
        locked_at = now_iso
        cur.execute(
            "UPDATE jobs SET state='processing', locked_by=?, locked_at=?, updated_at=? WHERE id=? AND (state='pending' OR state='failed')",
            (worker_id, locked_at, now_iso, job_id),
        )
        if cur.rowcount != 1:
            conn.commit()
            return None
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = dict(cur.fetchone())
        conn.commit()
        return job
    except Exception:
        conn.rollback()
        return None


def complete_job(job_id: str, output: Optional[str]):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    cur.execute("UPDATE jobs SET state='completed', updated_at=?, output=? WHERE id=?", (now, output, job_id))
    conn.commit()


def fail_job(job_id: str, attempts: int, max_retries: Optional[int], base: int, output: Optional[str]):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    # compute next_run_at
    if max_retries is None:
        max_retries = int(get_config("default-max-retries") or 3)
    if attempts >= max_retries:
        # move to dead
        cur.execute(
            "UPDATE jobs SET state='dead', attempts=?, updated_at=?, output=? WHERE id=?",
            (attempts, now, output, job_id),
        )
    else:
        delay = (base ** attempts)
        next_run = (datetime.utcnow()).timestamp() + delay
        next_run_iso = datetime.utcfromtimestamp(next_run).isoformat() + "Z"
        cur.execute(
            "UPDATE jobs SET state='failed', attempts=?, next_run_at=?, updated_at=?, output=? WHERE id=?",
            (attempts, next_run_iso, now, output, job_id),
        )
    conn.commit()


def get_job(job_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    r = cur.fetchone()
    return dict(r) if r else None


def move_dlq_to_pending(job_id: str):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat() + "Z"
    cur.execute("UPDATE jobs SET state='pending', attempts=0, next_run_at=NULL, updated_at=? WHERE id=?", (now, job_id))
    conn.commit()
