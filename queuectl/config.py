from . import db


def get(key: str):
    return db.get_config(key)


def set(key: str, value: str):
    db.set_config(key, value)
