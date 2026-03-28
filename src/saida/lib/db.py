# db.py
import os
import psycopg2

def _load_env_if_needed():
    # se já tem, não faz nada
    if os.getenv("DB_HOST") and os.getenv("DB_NAME") and os.getenv("DB_USER") and os.getenv("DB_PASSWORD"):
        return

    base_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_dir, ".env")

    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")


def get_conn():
    _load_env_if_needed()

    missing = [k for k in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"] if not os.getenv(k)]
    if missing:
        raise ValueError(f"Faltam variáveis no .env: {', '.join(missing)}")

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )