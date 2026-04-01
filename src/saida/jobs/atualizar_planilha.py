# ------------------------------------------------------------
# atualizar_planilha.py
# Atualiza planilha Google após lote FINALIZADO
# - Procura cabeçalho nas primeiras linhas
# - Mapeia colunas por sinônimos
# - Atualiza por chave (GRUPO + COTA)
# - OBSERVAÇÃO BOLETO só atualiza se a coluna existir
# ------------------------------------------------------------

import os
import re
import unicodedata
from typing import List, Tuple, Optional, Dict

import psycopg2

from lib.google_auth import criar_servico_sheets
from shared.log import log_info, log_erro


# =========================
# NORMALIZAÇÃO
# =========================
def _normalizar(texto: str) -> str:
    """Normaliza texto para comparar cabeçalhos."""
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
    with conn.cursor() as cur:
        cur.execute("""
            SELECT grupo, cota, status, observacao
            FROM tbl_fila_cotas
            WHERE id_fila_adm = %s
        """, (id_fila_adm,))
        return cur.fetchall()


# =========================
# CABEÇALHO
# =========================
def find_columns(header_row: List[str]) -> Tuple[int, int, int, Optional[int]]:
    """
    Obrigatórias:
      - GRUPO
      - COTA
      - BOLETO/STATUS
    Opcional:
      - OBSERVAÇÃO BOLETO / OBSERVAÇÃO
    """
    norm = [_normalizar(h) for h in header_row]

    def achar(*nomes: str) -> Optional[int]:
        for n in nomes:
            nn = _normalizar(n)
            if nn in norm:
                return norm.index(nn)
        return None

    idx_grupo = achar("GRUPO")
    idx_cota = achar("COTA")
    idx_boleto = achar("BOLETO", "STATUS")
    idx_obs_boleto = achar(
        "OBSERVAÇÃO BOLETO",
        "OBSERVACAO BOLETO",
        "OBSERVAÇÃO",
        "OBSERVACAO",
    )

    faltando = []
    if idx_grupo is None:
        faltando.append("GRUPO")
    if idx_cota is None:
        faltando.append("COTA")
    if idx_boleto is None:
        faltando.append("BOLETO/STATUS")

    if faltando:
        raise RuntimeError(
            f"Header faltando colunas: {', '.join(faltando)}. Header: {header_row}"
        )

    return idx_grupo, idx_cota, idx_boleto, idx_obs_boleto


def find_header_row(values: List[List[str]], max_linhas_busca: int = 10) -> Tuple[int, int, int, int, Optional[int]]:
    """
    Procura o cabeçalho nas primeiras linhas da planilha.
    Retorna:
      - índice da linha do cabeçalho (0-based)
      - idx_grupo
      - idx_cota
      - idx_boleto
      - idx_obs
    """
    limite = min(len(values), max_linhas_busca)

    for i in range(limite):
        row = values[i]
        if not any(str(c).strip() for c in row):
            continue

        try:
            idx_grupo, idx_cota, idx_boleto, idx_obs = find_columns(row)
            return i, idx_grupo, idx_cota, idx_boleto, idx_obs
        except RuntimeError:
            continue

    raise RuntimeError(f"Nenhum cabeçalho válido encontrado nas primeiras {limite} linhas.")


# =========================
# ATUALIZA ABA
# =========================
def atualizar_aba(
    service,
    spreadsheet_id: str,
    aba: str,
    cotas: List[Tuple[str, str, str, Optional[str]]],
    id_fila_adm: int,
    caminho_log: str
) -> Dict[str, int]:
    """
    Atualiza a aba por chave (grupo, cota).
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{aba}'!A1:ZZ",
        majorDimension="ROWS"
    ).execute()

    values = resp.get("values", [])
    if not values:
        return {
            "matched": 0,
            "updated_status": 0,
            "updated_obs": 0,
            "not_found": len(cotas),
            "duplicated_keys": 0
        }

    header_row_idx, idx_grupo, idx_cota, idx_boleto, idx_obs = find_header_row(values)

    index: Dict[Tuple[str, str], int] = {}
    duplicated = 0

    for i in range(header_row_idx + 1, len(values)):
        row = values[i]

        grupo_val = row[idx_grupo].strip() if idx_grupo < len(row) and row[idx_grupo] is not None else ""
        cota_val = row[idx_cota].strip() if idx_cota < len(row) and row[idx_cota] is not None else ""

        if not grupo_val or not cota_val:
            continue

        grupo_val = str(grupo_val).strip().zfill(6)
        cota_val = str(cota_val).strip().zfill(4)

        key = (grupo_val, cota_val)
        row_num = i + 1  # planilha é 1-based

        if key in index:
            duplicated += 1
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
        g = str(grupo or "").strip().zfill(6)
        c = str(cota or "").strip().zfill(4)

        if not g or not c:
            not_found += 1
            continue

        row_num = index.get((g, c))
        if not row_num:
            not_found += 1
            continue

        matched += 1

        updates.append({
            "range": f"'{aba}'!{col_boleto}{row_num}",
            "values": [[status if status is not None else ""]]
        })
        updated_status += 1

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
        log_erro(
            caminho_log=caminho_log,
            etapa="PLANILHA",
            id_dado=id_fila_adm,
            acao="Validar chaves duplicadas",
            detalhe=f"aba={aba} chaves duplicadas (GRUPO+COTA) detectadas: {duplicated}"
        )

    log_info(
        caminho_log=caminho_log,
        etapa="PLANILHA",
        id_dado=id_fila_adm,
        acao="Cabeçalho encontrado",
        detalhe=f"aba={aba} linha_cabecalho={header_row_idx + 1}"
    )

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
def atualizar_planilhas_finalizadas(id_fila_adm: int):
    service = criar_servico_sheets()
    conn = db_connect()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT f.id_adm, a.nome, a.link_planilha, a.nome_aba, f.caminho_log
                FROM tbl_fila_adm f
                JOIN tbl_adm a ON a.id_adm = f.id_adm
                WHERE f.id_fila_adm = %s
            """, (id_fila_adm,))
            row = cur.fetchone()

        if not row:
            raise RuntimeError(f"Lote {id_fila_adm} não encontrado")

        id_adm, nome, link_planilha, nome_aba_raw, caminho_log = row

        spreadsheet_id = extract_spreadsheet_id(link_planilha)
        if not spreadsheet_id:
            log_erro(
                caminho_log=caminho_log,
                etapa="PLANILHA",
                id_dado=id_fila_adm,
                acao="Validar planilha",
                detalhe=f"ADM={nome} sem spreadsheet_id"
            )
            return

        abas = [a.strip() for a in (nome_aba_raw or "").split(",") if a.strip()]
        if not abas:
            log_erro(
                caminho_log=caminho_log,
                etapa="PLANILHA",
                id_dado=id_fila_adm,
                acao="Validar abas",
                detalhe=f"ADM={nome} sem nome_aba"
            )
            return

        cotas = fetch_cotas(conn, id_fila_adm)

        for aba in abas:
            try:
                stats = atualizar_aba(
                    service=service,
                    spreadsheet_id=spreadsheet_id,
                    aba=aba,
                    cotas=cotas,
                    id_fila_adm=id_fila_adm,
                    caminho_log=caminho_log
                )

                log_info(
                    caminho_log=caminho_log,
                    etapa="PLANILHA",
                    id_dado=id_fila_adm,
                    acao="Atualizar aba",
                    detalhe=(
                        f"aba={aba} "
                        f"matched={stats['matched']} "
                        f"updated_status={stats['updated_status']} "
                        f"updated_obs={stats['updated_obs']} "
                        f"not_found={stats['not_found']} "
                        f"dup_keys={stats['duplicated_keys']}"
                    )
                )

            except Exception as e:
                log_erro(
                    caminho_log=caminho_log,
                    etapa="PLANILHA",
                    id_dado=id_fila_adm,
                    acao="Atualizar aba",
                    detalhe=f"aba={aba} erro={e}"
                )

    finally:
        conn.close()