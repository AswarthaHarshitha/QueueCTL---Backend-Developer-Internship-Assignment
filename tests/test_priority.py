import os
from queuectl import db


def test_priority_ordering(tmp_path):
    old_home = os.environ.get('HOME')
    os.environ['HOME'] = str(tmp_path)
    try:
        db.init_db()
        now = '2025-11-08T00:00:00Z'
        jobs = [
            {'id': 'low', 'command': 'echo low', 'priority': 1},
            {'id': 'high', 'command': 'echo high', 'priority': 10},
            {'id': 'mid', 'command': 'echo mid', 'priority': 5},
        ]
        for j in jobs:
            db.insert_job(j)
        picked1 = db.fetch_and_lock_job('t', now)
        assert picked1 and picked1['id'] == 'high'
        # mark as completed to free next
        db.complete_job(picked1['id'], 'ok')
        picked2 = db.fetch_and_lock_job('t2', now)
        assert picked2 and picked2['id'] == 'mid'
        db.complete_job(picked2['id'], 'ok')
        picked3 = db.fetch_and_lock_job('t3', now)
        assert picked3 and picked3['id'] == 'low'
    finally:
        if old_home is None:
            del os.environ['HOME']
        else:
            os.environ['HOME'] = old_home
