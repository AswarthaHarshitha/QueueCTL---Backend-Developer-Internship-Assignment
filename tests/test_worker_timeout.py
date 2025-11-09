import os
import time
import multiprocessing as mp
from queuectl import db
from queuectl import worker as worker_mod


def run_worker_process():
    # wrapper that calls the internal run function
    worker_mod._run_worker_process(0, int(db.get_config('backoff-base') or 2))


def test_worker_timeout_behavior(tmp_path):
    old_home = os.environ.get('HOME')
    os.environ['HOME'] = str(tmp_path)
    try:
        db.init_db()
        # set timeout to 1 second
        db.set_config('job-timeout', '1')
        # insert a job that sleeps for 2 seconds (will timeout)
        job = {'id': 'tmo', 'command': 'sleep 2', 'max_retries': 1}
        db.insert_job(job)
        # start worker in a separate process
        p = mp.Process(target=run_worker_process)
        p.start()
        # wait enough for worker to process and timeout
        time.sleep(3)
        p.terminate()
        p.join(timeout=1)
        j = db.get_job('tmo')
        assert j is not None
        # after timeout it should be failed (attempts >=1)
        assert j['state'] in ('failed', 'dead')
    finally:
        if old_home is None:
            del os.environ['HOME']
        else:
            os.environ['HOME'] = old_home
