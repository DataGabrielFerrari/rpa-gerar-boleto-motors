# lib/db.py
import os
import psycopg2


def get_conn():
    """
    Retorna uma conexão PostgreSQL usando variáveis do .env.
    Compatível com Aiven (SSL obrigatório).
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not host or not name or not user or not password:
        raise ValueError(
            "Faltam variáveis no .env: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD."
        )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
        sslmode="require"  # necessário para Aiven
    )