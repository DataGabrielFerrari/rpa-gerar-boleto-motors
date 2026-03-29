import os
import psycopg2

def get_conn():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    print("DB_HOST dentro do db.py:", host, flush=True)
    print("DB_PORT dentro do db.py:", port, flush=True)
    print("DB_NAME dentro do db.py:", name, flush=True)
    print("DB_USER dentro do db.py:", user, flush=True)

    if not host or not name or not user or not password:
        raise ValueError("Faltam variaveis no .env: host, name, user, password")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
        sslmode="require"
    )