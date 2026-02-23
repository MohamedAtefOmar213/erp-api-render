import os
import tds  # python-tds
from contextlib import contextmanager
from typing import List, Tuple, Optional, Any

AZURE_SERVER = os.getenv("AZURE_SERVER")  # erp-server-2026.database.windows.net
AZURE_DATABASE = os.getenv("AZURE_DATABASE")  # erp-db
AZURE_USERNAME = os.getenv("AZURE_USERNAME")  # sql user
AZURE_PASSWORD = os.getenv("AZURE_PASSWORD")  # sql password
AZURE_PORT = int(os.getenv("AZURE_PORT", "1433"))

if not all([AZURE_SERVER, AZURE_DATABASE, AZURE_USERNAME, AZURE_PASSWORD]):
    raise RuntimeError("Missing DB env vars: AZURE_SERVER/AZURE_DATABASE/AZURE_USERNAME/AZURE_PASSWORD")

@contextmanager
def get_conn():
    conn = tds.connect(server=AZURE_SERVER, database=AZURE_DATABASE,
                       user=AZURE_USERNAME, password=AZURE_PASSWORD,
                       port=AZURE_PORT, as_dict=False, timeout=30, appname="render-api")
    try:
        yield conn
    finally:
        conn.close()

def query_all(sql: str, params: Optional[List[Tuple[str, Any]]] = None):
    with get_conn() as c:
        with c.cursor() as cur:
            if params:
                # python-tds uses @name style
                cur.execute(sql, dict(params))
            else:
                cur.execute(sql)
            return cur.fetchall()

def query_one(sql: str, params: Optional[List[Tuple[str, Any]]] = None):
    with get_conn() as c:
        with c.cursor() as cur:
            if params:
                cur.execute(sql, dict(params))
            else:
                cur.execute(sql)
            return cur.fetchone()

def exec_write(sql: str, params: Optional[List[Tuple[str, Any]]] = None):
    with get_conn() as c:
        with c.cursor() as cur:
            if params:
                cur.execute(sql, dict(params))
            else:
                cur.execute(sql)
        c.commit()