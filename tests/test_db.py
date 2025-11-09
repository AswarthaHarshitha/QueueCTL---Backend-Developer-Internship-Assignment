import os
from queuectl import db


def test_db_init_and_insert(tmp_path):
    # Use a temporary home via env override
    old_home = os.environ.get('HOME')
    os.environ['HOME'] = str(tmp_path)
    try:
        conn = db.init_db()
        job = {
            'id': 't1',
            'command': 'echo hi',
            'max_retries': 2,
            'priority': 5,
        }
        db.insert_job(job)
        jobs = db.list_jobs(None)
        assert any(j['id'] == 't1' for j in jobs)
        # fetch and lock should return the job
        now = '2025-11-08T00:00:00Z'
        picked = db.fetch_and_lock_job('tester', now)
        assert picked is not None
        assert picked['id'] == 't1'
    finally:
        if old_home is None:
            del os.environ['HOME']
        else:
            os.environ['HOME'] = old_home
