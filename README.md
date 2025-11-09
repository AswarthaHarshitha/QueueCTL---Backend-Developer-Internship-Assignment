# queuectl

A minimal CLI-based background job queue system with retries, exponential backoff and a Dead Letter Queue (DLQ).

## Features

- Enqueue jobs with a shell command payload
- Start workers (foreground or background daemon) to execute jobs in parallel
- Retry failed jobs with exponential backoff
- Move permanently failed jobs to Dead Letter Queue
- Persistent storage using SQLite
- Simple CLI for management and configuration

This is a lightweight implementation intended for the assignment. See usage and examples below.

## Unique/extra features added

- Job timeouts: workers enforce a per-job timeout (config key `job-timeout`) and fail jobs that exceed it.
- Job priority: jobs accept a numeric `priority` (higher is processed first).
- Per-job tags and scheduling: jobs may include `tags` and an optional `run_at` timestamp to schedule future runs.
- Per-job logs: worker writes stdout/stderr (and timeout messages) to `~/.queuectl/logs/<job_id>.log`.
- Metrics endpoint: a tiny HTTP server exposes job counts at `/metrics` (JSON) via `queuectl metrics serve --port`.

## Setup

Requirements: Python 3.8+

Install (optional) into a virtualenv and install dependences (none external required):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Make the script executable (optional):

```bash
chmod +x bin/queuectl
```

By default the DB is created at `~/.queuectl/queue.db` and runtime files live in `~/.queuectl`.

## Usage examples

Enqueue a job with JSON inline:

```bash
./bin/queuectl enqueue '{"id":"job1","command":"echo hello","max_retries":3}'
```

List pending jobs:

```bash
./bin/queuectl list --state pending
```

Start 3 workers in background (daemon):

```bash
./bin/queuectl worker start --count 3 --daemon
```

Stop background workers:

```bash
./bin/queuectl worker stop
```

Show status summary:

```bash
./bin/queuectl status
```

View DLQ:

```bash
./bin/queuectl dlq list
./bin/queuectl dlq retry job1
```

Set config values:

```bash
./bin/queuectl config set backoff-base 2
./bin/queuectl config set default-max-retries 3
```

## Architecture Overview

- Storage: SQLite at `~/.queuectl/queue.db` with `jobs` and `config` tables.
- Worker: A master process (when started with `--daemon`) spawns worker processes. Workers atomically pick a job by transitioning its state to `processing` inside a transaction to avoid duplicates.
- Retry/backoff: After a failed run the job `attempts` is incremented and `next_run_at` is set to now + base^attempts seconds. When `attempts` > `max_retries` the job is moved to `dead` state (DLQ).

## Assumptions & Trade-offs

- This is a minimal assignment implementation focused on correctness and clarity, not extreme scalability.
- Background daemon is implemented with a PID file under `~/.queuectl/pid` and uses Python multiprocessing to spawn worker processes.
- Job output is stored in the `output` column limited to what the process prints.

## Testing

A small test script is provided to exercise basic flows, including success, retry and DLQ. See `scripts/test_flow.sh`.

## Files added

- `bin/queuectl` - executable wrapper
- `queuectl/__main__.py` - entrypoint for `python -m queuectl`
- `queuectl/cli.py`, `queuectl/db.py`, `queuectl/worker.py`, `queuectl/config.py` - core logic
- `scripts/test_flow.sh` - simple test harness

## Notes

This implementation was created to satisfy the assignment requirements: enqueue, multiple workers, persistence, retry/backoff, DLQ and CLI management. See code for more details.
# queuectl

A minimal CLI-based background job queue system with retries, exponential backoff and a Dead Letter Queue (DLQ).

Features
- Enqueue jobs with a shell command payload
- Start workers (foreground or background daemon) to execute jobs in parallel
- Retry failed jobs with exponential backoff
- Move permanently failed jobs to Dead Letter Queue
- Persistent storage using SQLite
- Simple CLI for management and configuration

This is a lightweight implementation intended for the assignment. See usage and examples below.

Unique/extra features added
- Job timeouts: workers enforce a per-job timeout (config key `job-timeout`) and fail jobs that exceed it.
- Job priority: jobs accept a numeric `priority` (higher is processed first).
- Per-job tags and scheduling: jobs may include `tags` and an optional `run_at` timestamp to schedule future runs.
- Per-job logs: worker writes stdout/stderr (and timeout messages) to `~/.queuectl/logs/<job_id>.log`.
- Metrics endpoint: a tiny HTTP server exposes job counts at `/metrics` (JSON) via `queuectl metrics serve --port`.

## Setup

Requirements: Python 3.8+

Install (optional) into a virtualenv and install dependences (none external required):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Make the script executable (optional):

```bash
chmod +x bin/queuectl
```

By default the DB is created at `~/.queuectl/queue.db` and runtime files live in `~/.queuectl`.

## Usage examples

Enqueue a job with JSON inline:

```bash
./bin/queuectl enqueue '{"id":"job1","command":"echo hello","max_retries":3}'
```

List pending jobs:

```bash
./bin/queuectl list --state pending
```

Start 3 workers in background (daemon):

```bash
./bin/queuectl worker start --count 3 --daemon
```

Stop background workers:

```bash
./bin/queuectl worker stop
```

Show status summary:

```bash
./bin/queuectl status
```

View DLQ:

```bash
./bin/queuectl dlq list
./bin/queuectl dlq retry job1
```

Set config values:

```bash
./bin/queuectl config set backoff-base 2
./bin/queuectl config set default-max-retries 3
```

## Architecture Overview

- Storage: SQLite at `~/.queuectl/queue.db` with `jobs` and `config` tables.
- Worker: A master process (when started with `--daemon`) spawns worker processes. Workers atomically pick a job by transitioning its state to `processing` inside a transaction to avoid duplicates.
- Retry/backoff: After a failed run the job `attempts` is incremented and `next_run_at` is set to now + base^attempts seconds. When `attempts` > `max_retries` the job is moved to `dead` state (DLQ).

## Assumptions & Trade-offs

- This is a minimal assignment implementation focused on correctness and clarity, not extreme scalability.
- Background daemon is implemented with a PID file under `~/.queuectl/pid` and uses Python multiprocessing to spawn worker processes.
- Job output is stored in the `output` column limited to what the process prints.

## Testing

A small test script is provided to exercise basic flows, including success, retry and DLQ. See `scripts/test_flow.sh`.

## Files added

- `bin/queuectl` - executable wrapper
- `queuectl/__main__.py` - entrypoint for `python -m queuectl`
- `queuectl/cli.py`, `queuectl/db.py`, `queuectl/worker.py`, `queuectl/config.py` - core logic
- `scripts/test_flow.sh` - simple test harness

## Notes

This implementation was created to satisfy the assignment requirements: enqueue, multiple workers, persistence, retry/backoff, DLQ and CLI management. See code for more details.