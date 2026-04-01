import re
import unicodedata

def normalizar(texto: str) -> str:
    if texto is None:
        return ""
    t = str(texto).strip().lower()
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"\s+", "_", t)
    t = re.sub(r"[^a-z0-9_]", "", t)
    return t

def split_abas(nome_aba: str):
    abas = [a.strip() for a in (nome_aba or "").split(",") if a.strip()]
    if len(abas) < 1:
        raise ValueError("nome_aba precisa ter pelo menos 1 aba.")
    return abas