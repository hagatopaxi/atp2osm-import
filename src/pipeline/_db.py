import os

import psycopg


def connect():
    return psycopg.connect(
        dbname=os.getenv("OSM_DB_NAME"),
        user=os.getenv("OSM_DB_USER"),
        password=os.getenv("OSM_DB_PASSWORD"),
        host=os.getenv("OSM_DB_HOST"),
        port=os.getenv("OSM_DB_PORT"),
    )


def last_import_date(conn, import_type):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date FROM data_imports WHERE type=%s ORDER BY date DESC LIMIT 1",
            (import_type,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def record_import(conn, import_type, date, status):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO data_imports (type, date, status) VALUES (%s, %s, %s)",
            (import_type, date, status),
        )
    conn.commit()
