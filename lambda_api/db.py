"""
db.py — Conexión a RDS PostgreSQL
"""

import os
import psycopg2
import psycopg2.pool

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.environ.get("DB_HOST", "shopstream-dw.cmrjmn8s79od.us-east-1.rds.amazonaws.com"),
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ.get("DB_NAME", "shopstream_dw"),
            user=os.environ.get("DB_USER", "shopstream"),
            password=os.environ.get("DB_PASSWORD", "ShopStream2024!"),
            connect_timeout=10,
        )
    return _pool


def query_to_list(sql, params=()):
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        pool.putconn(conn)
