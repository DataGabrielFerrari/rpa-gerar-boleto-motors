# ------------------------------------------------------------
# atualizar_planilha.py
# Atualiza planilha Google após lote FINALIZADO
# - Mapeia colunas por sinônimos (igual leitor)
# - Atualiza por chave (GRUPO + COTA) para não atualizar linha errada
# - OBSERVAÇÃO BOLETO só atualiza se a coluna existir (nome completo)
# ------------------------------------------------------------

import os
import re
import unicodedata
import logging
from typing import List, Tuple, Optional, Dict

import psycopg2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


# =========================
# SCOPES
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# =========================
# NORMALIZAÇÃO (igual leitor)
# =========================
def _normalizar(texto: str) -> str:
    """Normaliza texto para comparar cabeçalhos (sem acento, minúsculo, sem símbolos)."""
    if texto is None:
        return ""
    t = str(texto).strip().lower()
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"\s+", "_", t)
    t = re.sub(r"[^a-z0-9_]", "", t)
    return t


def col_to_letter(col_idx_1based: int) -> str:
    s = ""
    n = col_idx_1based
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def extract_spreadsheet_id(link_or_id: str) -> str:
    if not link_or_id:
        return ""

    if "/" not in link_or_id and len(link_or_id) > 20:
        return link_or_id.strip()

    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", link_or_id)
    if m:
        return m.group(1)

    return link_or_id.strip()


# =========================
# GOOGLE SERVICE
# =========================
def get_sheets_service(base_dir: str):
    token_path = os.path.join(base_dir, "credentials", "token.json")
    client_secret_path = os.path.join(base_dir, "credentials", "client_secret.json")

    if not os.path.exists(client_secret_path):
        raise FileNotFoundError(f"client_secret.json não encontrado em: {client_secret_path}")

    creds: Optional[Credentials] = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)

        os.makedirs(os.path.dirname(token_path), exist_ok=True)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# =========================
# DB
# =========================
def db_connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


# =========================
# QUERIES
# =========================
def fetch_lotes_para_atualizar(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id_fila_adm, id_adm
            FROM tbl_fila_adm
            WHERE status = 'FINALIZADO'
              AND link_drive IS NOT NULL
            ORDER BY id_fila_adm
        """)
        return cur.fetchall()


def fetch_adm_info(conn, id_adm: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nome, link_planilha, nome_aba
            FROM tbl_adm
            WHERE id_adm = %s
        """, (id_adm,))
        return cur.fetchone()


def fetch_cotas(conn, id_fila_adm: int):
    """
    Retorna: grupo, cota, status, observacao
    - status: o texto que você quer escrever no BOLETO/STATUS na planilha
    - observacao: texto para OBSERVAÇÃO BOLETO (se existir a coluna)
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT grupo, cota, status, observacao
            FROM tbl_fila_cotas
            WHERE id_fila_adm = %s
        """, (id_fila_adm,))
        return cur.fetchall()


# =========================
# LOCALIZAÇÃO DE COLUNAS (com sinônimos)
# =========================
def find_columns(header_row: List[str]) -> Tuple[int, int, int, Optional[int]]:
    """
    Obrigatórias:
      - GRUPO
      - COTA
      - BOLETO/STATUS
    Opcional:
      - OBSERVAÇÃO BOLETO (nome completo)
    """
    norm = [_normalizar(h) for h in header_row]

    def achar(*nomes: str) -> Optional[int]:
        for n in nomes:
            nn = _normalizar(n)
            if nn in norm:
                return norm.index(nn)
        return None

    idx_grupo = achar("GRUPO")
    idx_cota = achar("COTA")  # saída atualiza por GRUPO+COTA
    idx_boleto = achar("BOLETO", "STATUS")
    idx_obs_boleto = achar("OBSERVAÇÃO BOLETO", "OBSERVACAO BOLETO")

    faltando = []
    if idx_grupo is None:
        faltando.append("GRUPO")
    if idx_cota is None:
        faltando.append("COTA")
    if idx_boleto is None:
        faltando.append("BOLETO/STATUS")

    if faltando:
        raise RuntimeError(f"Header faltando colunas: {', '.join(faltando)}. Header: {header_row}")

    return idx_grupo, idx_cota, idx_boleto, idx_obs_boleto


# =========================
# ATUALIZA ABA
# =========================
def atualizar_aba(service, spreadsheet_id: str, aba: str, cotas: List[Tuple[str, str, str, Optional[str]]], logger: logging.Logger) -> Dict[str, int]:
    """
    Atualiza a aba por chave (grupo, cota).

    Retorna dict com contadores:
      - matched: quantas (grupo,cota) encontrou na planilha
      - updated_status: quantas células de status atualizou
      - updated_obs: quantas células de observação atualizou
      - not_found: quantas do DB não achou na planilha
      - duplicated_keys: chaves duplicadas detectadas na planilha
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{aba}'!A1:ZZ",
        majorDimension="ROWS"
    ).execute()

    values = resp.get("values", [])
    if not values:
        return {"matched": 0, "updated_status": 0, "updated_obs": 0, "not_found": len(cotas), "duplicated_keys": 0}

    header = values[0]
    idx_grupo, idx_cota, idx_boleto, idx_obs = find_columns(header)

    # Index por (grupo, cota) -> row_num
    index: Dict[Tuple[str, str], int] = {}
    duplicated = 0

    for i in range(1, len(values)):
        row = values[i]

        grupo_val = (row[idx_grupo].strip() if idx_grupo < len(row) and row[idx_grupo] is not None else "")
        cota_val = (row[idx_cota].strip() if idx_cota < len(row) and row[idx_cota] is not None else "")

        if not grupo_val or not cota_val:
            continue

        key = (grupo_val, cota_val)
        row_num = i + 1

        if key in index:
            duplicated += 1
            # mantém a primeira ocorrência; loga a duplicidade
            continue

        index[key] = row_num

    col_boleto = col_to_letter(idx_boleto + 1)
    col_obs = col_to_letter(idx_obs + 1) if idx_obs is not None else None

    updates = []
    matched = 0
    updated_status = 0
    updated_obs = 0
    not_found = 0

    for grupo, cota, status, obs in cotas:
        g = (grupo or "").strip()
        c = (cota or "").strip()

        if not g or not c:
            not_found += 1
            continue

        row_num = index.get((g, c))
        if not row_num:
            not_found += 1
            continue

        matched += 1

        # Atualiza STATUS/BOLETO
        updates.append({
            "range": f"'{aba}'!{col_boleto}{row_num}",
            "values": [[status if status is not None else ""]]
        })
        updated_status += 1

        # Atualiza OBSERVAÇÃO BOLETO (se existir coluna)
        if col_obs is not None:
            updates.append({
                "range": f"'{aba}'!{col_obs}{row_num}",
                "values": [[obs if obs is not None else ""]]
            })
            updated_obs += 1

    if updates:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": updates
            }
        ).execute()

    if duplicated > 0:
        logger.warning(f"[PLANILHA] aba={aba} chaves duplicadas (GRUPO+COTA) detectadas: {duplicated}")

    return {
        "matched": matched,
        "updated_status": updated_status,
        "updated_obs": updated_obs,
        "not_found": not_found,
        "duplicated_keys": duplicated
    }


# =========================
# FUNÇÃO PRINCIPAL EXPORTADA
# =========================
def atualizar_planilhas_finalizadas(logger: logging.Logger):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    service = get_sheets_service(base_dir)
    conn = db_connect()

    try:
        lotes = fetch_lotes_para_atualizar(conn)

        if not lotes:
            logger.info("Nenhum lote FINALIZADO para atualizar planilha.")
            return

        for id_fila_adm, id_adm in lotes:
            try:
                adm_info = fetch_adm_info(conn, id_adm)
                if not adm_info:
                    logger.error(f"[ERRO PLANILHA] lote={id_fila_adm} | ADM id_adm={id_adm} não encontrado.")
                    continue

                nome, link_planilha, nome_aba_raw = adm_info

                spreadsheet_id = extract_spreadsheet_id(link_planilha)
                if not spreadsheet_id:
                    logger.error(f"[ERRO PLANILHA] lote={id_fila_adm} | ADM={nome} sem spreadsheet_id.")
                    continue

                abas = [a.strip() for a in (nome_aba_raw or "").split(",") if a.strip()]
                if not abas:
                    logger.error(f"[ERRO PLANILHA] lote={id_fila_adm} | ADM={nome} sem nome_aba.")
                    continue

                cotas = fetch_cotas(conn, id_fila_adm)

                for aba in abas:
                    stats = atualizar_aba(service, spreadsheet_id, aba, cotas, logger)
                    logger.info(
                        f"[PLANILHA OK] ADM={nome} | lote={id_fila_adm} | aba={aba} | "
                        f"matched={stats['matched']} updated_status={stats['updated_status']} "
                        f"updated_obs={stats['updated_obs']} not_found={stats['not_found']} dup_keys={stats['duplicated_keys']}"
                    )

            except Exception as e:
                logger.error(f"[ERRO PLANILHA] lote={id_fila_adm} | {e}")

    finally:
        conn.close()