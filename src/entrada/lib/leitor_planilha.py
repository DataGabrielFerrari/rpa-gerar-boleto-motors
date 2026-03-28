# jobs/leitor_planilha.py
# ------------------------------------------------------------
# Lê 1+ abas (tbl_adm.nome_aba = "IMÓVEL" ou "IMÓVEL,LOTRANS")
# Normal (modo_reexecucao=False):
#   - BOLETO diferente de (DDA, CC, CANCELADO, NÃO PROCESSAR) vira "NÃO BAIXADO"
#   - Enfileira TODAS as linhas não bloqueadas
# Reexecução (modo_reexecucao=True):
#   - NÃO atualiza planilha
#   - Só enfileira as linhas que já estão "NÃO BAIXADO" (o consultor marcou manualmente)
# Grava a aba de origem em tbl_fila_cotas.nome_aba (ex: MOTORS / LOTRANS)
# ------------------------------------------------------------

import os
import re
import unicodedata
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from lib.db import get_conn

log = logging.getLogger(__name__)

# Precisa de escrita no Sheets para marcar "NÃO BAIXADO" (modo normal)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]

# =========================
# Utilitários
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


def _append_log(log_txt_path: Optional[str], level: str, msg: str) -> None:
    """Append no log.txt do lote (não quebra execução se falhar)."""
    if not log_txt_path:
        return
    try:
        os.makedirs(os.path.dirname(log_txt_path), exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_txt_path, "a", encoding="utf-8") as f:
            f.write(f"{ts} | {level.upper()} | {msg}\n")
    except Exception:
        pass


def _split_abas(nome_aba: str) -> List[str]:
    """Aceita 1 ou mais abas: 'IMÓVEL' ou 'IMÓVEL,LOTRANS'."""
    abas = [a.strip() for a in (nome_aba or "").split(",") if a.strip()]
    if len(abas) < 1:
        raise ValueError("nome_aba precisa ter pelo menos 1 aba (ex: 'IMÓVEL' ou 'IMÓVEL,LOTRANS').")
    return abas


def _extrair_id_planilha(link: str) -> str:
    """Extrai o ID do link do Google Sheets."""
    if not link:
        raise ValueError("link_planilha está vazio.")
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", link)
    if not m:
        raise ValueError(f"Não consegui extrair o ID da planilha do link: {link}")
    return m.group(1)


def _servico_sheets() -> object:
    """Autentica usando credentials/client_secret.json e credentials/token.json."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    cred_dir = os.path.join(base_dir, "credentials")
    client_secret = os.path.join(cred_dir, "client_secret.json")
    token_path = os.path.join(cred_dir, "token.json")

    if not os.path.exists(client_secret):
        raise FileNotFoundError(f"Não achei: {client_secret}")

    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret, SCOPES)
            creds = flow.run_local_server(port=0)

        os.makedirs(cred_dir, exist_ok=True)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)


def _coluna_para_letra(idx_zero_based: int) -> str:
    """0->A, 1->B, ..., 25->Z, 26->AA ..."""
    idx = idx_zero_based + 1
    letras = ""
    while idx > 0:
        idx, resto = divmod(idx - 1, 26)
        letras = chr(65 + resto) + letras
    return letras


# =========================
# Regras do BOLETO
# =========================

BLOQUEADOS = {
    "DDA",
    "CC",
    "CANCELADO",
    "NAO PROCESSAR",
    "NÃO PROCESSAR",
}


def _status_boleto(texto: str) -> str:
    """Normaliza o status do BOLETO para comparar."""
    return (texto or "").strip().upper()


# =========================
# Leitura / processamento
# =========================

def _mapear_indices_cabecalho(cabecalho: List[str]) -> Dict[str, int]:
    """
    Localiza as colunas pelo header (sem depender de letra).
    Obrigatórias:
      - GRUPO
      - COTA
      - CONSULTOR
      - BOLETO
      - NOME DO CLIENTE
    """
    norm = [_normalizar(h) for h in cabecalho]

    def achar(*nomes: str) -> Optional[int]:
        for n in nomes:
            nn = _normalizar(n)
            if nn in norm:
                return norm.index(nn)
        return None

    idx_grupo = achar("GRUPO")
    idx_cota = achar("COTA")
    idx_consultor = achar("CONSULTOR", "NOME DA PASTA", "NOME_DA_PASTA", "PASTA")
    idx_boleto = achar("BOLETO", "STATUS")
    idx_cliente = achar("NOME DO CLIENTE","NOME DE CLIENTE", "NOME_CLIENTE", "CLIENTE", "CONSORCIADO")
    idx_obs_boleto = achar(
        "OBSERVAÇÃO BOLETO",
        "OBSERVACAO BOLETO"
    )

    faltando = []
    if idx_grupo is None:
        faltando.append("GRUPO")

    if idx_cota is None:
        faltando.append("COTA")

    if idx_consultor is None:
        faltando.append("CONSULTOR / NOME DA PASTA")

    if idx_boleto is None:
        faltando.append("BOLETO / STATUS")

    if idx_cliente is None:
        faltando.append("NOME DO CLIENTE")

    if faltando:
        raise ValueError(f"Header faltando colunas: {', '.join(faltando)}. Header recebido: {cabecalho}")

    return {
        "grupo": idx_grupo,
        "cota": idx_cota,
        "consultor": idx_consultor,
        "boleto": idx_boleto,
        "cliente": idx_cliente,
        "obs_boleto": idx_obs_boleto
    }


def _ler_range(service, spreadsheet_id: str, range_a1: str) -> List[List[str]]:
    """Lê valores de um range A1."""
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        majorDimension="ROWS"
    ).execute()
    return resp.get("values", [])


def _atualizar_boleto_em_lote(service, spreadsheet_id: str, aba: str, letra_col_boleto: str, linhas: List[int]) -> None:
    """Atualiza a coluna do STATUS/BOLETO para 'NÃO BAIXADO' nas linhas informadas."""
    if not linhas:
        return

    data = []
    for row_num in linhas:
        rng = f"{aba}!{letra_col_boleto}{row_num}"
        data.append({"range": rng, "values": [["NÃO BAIXADO"]]})

    body = {"valueInputOption": "RAW", "data": data}

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()


def ler_planilhas(
    id_fila_adm: int,
    modo_reexecucao: bool,
    log_txt_path: Optional[str] = None
) -> int:
    """
    1) Busca ADM/lote no banco
    2) Para cada aba:
       - lê uma faixa larga (A:Z) para garantir pegar as colunas pelo header
       - modo normal: prepara BOLETO (seta 'NÃO BAIXADO' quando aplicável)
       - modo reexecução: NÃO altera planilha, só pega 'NÃO BAIXADO'
       - enfileira só as linhas que devem ser processadas
    3) Atualiza total_cotas no lote
    Retorna total inserido.
    """
    conn = get_conn()
    cur = conn.cursor()

    _append_log(log_txt_path, "INFO", f"[PY] Leitor iniciado | id_fila_adm={id_fila_adm} reexecucao={modo_reexecucao}")

    # Busca infos do lote + ADM
    cur.execute("""
        SELECT f.id_adm, f.mes_ref, a.link_planilha, a.nome_aba
        FROM tbl_fila_adm f
        JOIN tbl_adm a ON a.id_adm = f.id_adm
        WHERE f.id_fila_adm = %s
    """, (id_fila_adm,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise ValueError(f"id_fila_adm não encontrado: {id_fila_adm}")

    id_adm, mes_ref, link_planilha, nome_aba = row

    abas = _split_abas(nome_aba)
    spreadsheet_id = _extrair_id_planilha(link_planilha)
    service = _servico_sheets()

    itens_para_inserir: List[Tuple] = []

    # contadores (auditoria)
    total_linhas_lidas = 0
    total_invalidas = 0
    total_bloqueadas = 0
    total_filtradas_reexec = 0
    total_enfileiradas = 0
    total_abaspuladas = 0

    for aba in abas:
        try:
            _append_log(log_txt_path, "INFO", f"[PY] Lendo aba: {aba}")

            # Range amplo para achar as colunas pelo header (se sua planilha for maior, aumente para A:AZ)
            valores = _ler_range(service, spreadsheet_id, f"{aba}!A:Z")

            if not valores or len(valores) < 2:
                total_abaspuladas += 1
                msg = f"[LEITOR] aba={aba} sem dados suficientes."
                log.info(msg)
                _append_log(log_txt_path, "WARNING", f"[PY] {msg}")
                continue

            cabecalho = valores[0]
            idx = _mapear_indices_cabecalho(cabecalho)

            letra_boleto = _coluna_para_letra(idx["boleto"])
            linhas_para_atualizar: List[int] = []  # só usado no modo normal

            for i, r in enumerate(valores[1:], start=2):
                total_linhas_lidas += 1

                def cell(j: int) -> str:
                    return (r[j] if j < len(r) else "").strip() if r else ""

                nome_cliente = cell(idx["cliente"])
                grupo = cell(idx["grupo"])
                cota = cell(idx["cota"])
                consultor = cell(idx["consultor"])
                boleto = cell(idx["boleto"])

                # OBSERVAÇÃO BOLETO (coluna opcional)
                observacao_boleto = None
                if idx.get("obs_boleto") is not None:
                    obs_raw = cell(idx["obs_boleto"])
                    observacao_boleto = obs_raw if obs_raw else None

                # inválida -> ignora
                if not nome_cliente or not grupo or not cota or not consultor:
                    total_invalidas += 1
                    continue

                status_atual = _status_boleto(boleto)

                # bloqueados -> não mexe e não enfileira
                if status_atual in BLOQUEADOS:
                    total_bloqueadas += 1
                    continue

                # REEXECUÇÃO: só pega o que já está NÃO BAIXADO
                if modo_reexecucao:
                    if status_atual not in ("NÃO BAIXADO", "NAO BAIXADO"):
                        total_filtradas_reexec += 1
                        continue
                else:
                    # NORMAL/ADIANTADO: tudo que não for bloqueado deve ficar como "NÃO BAIXADO"
                    if status_atual not in ("NÃO BAIXADO", "NAO BAIXADO"):
                        linhas_para_atualizar.append(i)

                # enfileira
                itens_para_inserir.append((
                    id_fila_adm,
                    id_adm,
                    consultor,
                    nome_cliente,
                    grupo,
                    cota,
                    observacao_boleto,
                    aba,  # nome_aba (origem)
                ))
                total_enfileiradas += 1

            # atualizar BOLETO na coluna correta (somente no modo normal)
            if not modo_reexecucao:
                _append_log(
                    log_txt_path,
                    "INFO",
                    f"[PY] Atualizando BOLETO -> 'NÃO BAIXADO' | aba={aba} qtd={len(linhas_para_atualizar)}"
                )
                _atualizar_boleto_em_lote(service, spreadsheet_id, aba, letra_boleto, linhas_para_atualizar)

        except Exception as e:
            # não derruba o lote inteiro por uma aba; loga e continua
            total_abaspuladas += 1
            msg = f"[LEITOR ERRO] aba={aba} erro={e}"
            log.exception(msg)
            _append_log(log_txt_path, "ERROR", f"[PY] {msg}")

    # Insere no banco
    if itens_para_inserir:
        cur.executemany("""
            INSERT INTO tbl_fila_cotas
            (
                id_fila_adm,
                id_adm,
                nome_consultor,
                nome_cliente,
                grupo,
                cota,
                parcelas_atraso,
                status,
                tentativas,
                observacao,
                caminho_boleto,
                caminho_print,
                atualizado_em,
                nome_aba
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s,
                0,
                'PENDENTE',
                0,
                %s,
                NULL,
                NULL,
                NOW(),
                %s
            )
        """, itens_para_inserir)

    total = len(itens_para_inserir)

    # Atualiza total no lote
    cur.execute("""
        UPDATE tbl_fila_adm
        SET total_cotas = %s
        WHERE id_fila_adm = %s
    """, (total, id_fila_adm))

    conn.commit()
    cur.close()
    conn.close()

    resumo = (
        f"[LEITOR RESUMO] id_fila_adm={id_fila_adm} mes_ref={mes_ref} reexec={modo_reexecucao} "
        f"abas={len(abas)} abas_puladas={total_abaspuladas} lidas={total_linhas_lidas} invalidas={total_invalidas} "
        f"bloqueadas={total_bloqueadas} filtradas_reexec={total_filtradas_reexec} enfileiradas={total_enfileiradas}"
    )

    log.info(resumo)
    _append_log(log_txt_path, "INFO", f"[PY] {resumo}")
    _append_log(log_txt_path, "INFO", f"[PY] Leitor finalizado | total_enfileiradas={total}")

    return total