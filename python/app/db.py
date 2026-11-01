"""Postgres connection pool (psycopg 3)."""
from psycopg_pool import ConnectionPool

from app import config

_pool = None


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(config.DB_DSN, min_size=1, max_size=10, open=True, timeout=5)
    return _pool


def db_is_up() -> bool:
    try:
        with get_pool().connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False
