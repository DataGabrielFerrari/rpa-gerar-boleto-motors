import os
import sys
import logging
from typing import List, Optional, Tuple

# =========================
# PATHS
# =========================
CURRENT_DIR = os.path.dirname(__file__)                  # src/entrada/jobs
ENTRADA_DIR = os.path.dirname(CURRENT_DIR)              # src/entrada
SRC_DIR = os.path.dirname(ENTRADA_DIR)                  # src

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

if ENTRADA_DIR not in sys.path:
    sys.path.insert(0, ENTRADA_DIR)

# =========================
# IMPORTS DO PROJETO
# =========================
from lib.db import get_conn
from lib.google_auth import criar_servico_sheets
from lib.boleto_rules import status_boleto, deve_bloquear, esta_nao_baixado
from utils.texto_utils import split_abas
from utils.cabecalho_utils import mapear_indices_cabecalho, encontrar_cabecalho
from utils.sheets_utils import (
    extrair_id_planilha,
    ler_range,
    coluna_para_letra,
    atualizar_boleto_em_lote,
)
from shared.log import log_info, log_erro


log = logging.getLogger(__name__)


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

    log_info(
        caminho_log=log_txt_path,
        etapa="LEITOR_PLANILHA",
        id_dado=id_fila_adm,
        acao="Iniciar leitura",
        detalhe=f"modo_reexecucao={modo_reexecucao}"
    )

    try:
        # =========================
        # BUSCA DADOS DO LOTE
        # =========================
        cur.execute("""
            SELECT f.id_adm, f.mes_ref, a.link_planilha, a.nome_aba
            FROM tbl_fila_adm f
            JOIN tbl_adm a ON a.id_adm = f.id_adm
            WHERE f.id_fila_adm = %s
        """, (id_fila_adm,))
        row = cur.fetchone()

        if not row:
            raise ValueError(f"id_fila_adm não encontrado: {id_fila_adm}")

        id_adm, mes_ref, link_planilha, nome_aba = row

        abas = split_abas(nome_aba)
        spreadsheet_id = extrair_id_planilha(link_planilha)
        service = criar_servico_sheets()

        itens_para_inserir: List[Tuple] = []

        # =========================
        # CONTADORES
        # =========================
        total_linhas_lidas = 0
        total_invalidas = 0
        total_bloqueadas = 0
        total_filtradas_reexec = 0
        total_enfileiradas = 0
        total_abas_puladas = 0

        # =========================
        # PROCESSA CADA ABA
        # =========================
        for aba in abas:
            try:
                log_info(
                    caminho_log=log_txt_path,
                    etapa="LEITOR_PLANILHA",
                    id_dado=id_fila_adm,
                    acao="Ler aba",
                    detalhe=f"aba={aba}"
                )

                valores = ler_range(service, spreadsheet_id, f"{aba}!A:Z")

                if not valores:
                    total_abas_puladas += 1
                    msg = f"aba={aba} sem dados"
                    log.warning(msg)

                    log_erro(
                        caminho_log=log_txt_path,
                        etapa="LEITOR_PLANILHA",
                        id_dado=id_fila_adm,
                        acao="Ler aba",
                        detalhe=msg
                    )
                    continue

                idx_cabecalho, idx = encontrar_cabecalho(valores)
                cabecalho = valores[idx_cabecalho]

                log_info(
                    caminho_log=log_txt_path,
                    etapa="LEITOR_PLANILHA",
                    id_dado=id_fila_adm,
                    acao="Encontrar cabeçalho",
                    detalhe=f"aba={aba} linha_cabecalho={idx_cabecalho + 1}"
                )

                if len(valores) <= idx_cabecalho + 1:
                    total_abas_puladas += 1
                    msg = f"aba={aba} sem linhas de dados abaixo do cabeçalho"
                    log.warning(msg)

                    log_erro(
                        caminho_log=log_txt_path,
                        etapa="LEITOR_PLANILHA",
                        id_dado=id_fila_adm,
                        acao="Ler aba",
                        detalhe=msg
                    )
                    continue

                letra_boleto = coluna_para_letra(idx["boleto"])
                linhas_para_atualizar: List[int] = []

                for i, r in enumerate(valores[idx_cabecalho + 1:], start=idx_cabecalho + 2):
                    total_linhas_lidas += 1

                    def cell(j: int) -> str:
                        return (r[j] if j < len(r) else "").strip() if r else ""

                    nome_cliente = cell(idx["cliente"])
                    grupo = cell(idx["grupo"])
                    cota = cell(idx["cota"])
                    consultor = (cell(idx["consultor"]) if idx.get("consultor") is not None else "").strip() or "Boletos"
                    boleto = cell(idx["boleto"])

                    pode_unificar = None
                    if idx.get("pode_unificar") is not None:
                        pode_unificar_raw = cell(idx["pode_unificar"])
                        pode_unificar = pode_unificar_raw if pode_unificar_raw else None

                    observacao_boleto = None
                    if idx.get("obs_boleto") is not None:
                        obs_raw = cell(idx["obs_boleto"])
                        observacao_boleto = obs_raw if obs_raw else None

                    # inválida -> ignora
                    if not nome_cliente or not grupo or not cota:
                        total_invalidas += 1
                        continue

                    status_atual = status_boleto(boleto)

                    if deve_bloquear(status_atual):
                        total_bloqueadas += 1
                        continue

                    if modo_reexecucao:
                        if not esta_nao_baixado(status_atual):
                            total_filtradas_reexec += 1
                            continue
                    else:
                        if not esta_nao_baixado(status_atual):
                            linhas_para_atualizar.append(i)

                    # enfileira
                    grupo = str(grupo).zfill(6)
                    cota = str(cota).zfill(4)
                    itens_para_inserir.append((
                        id_fila_adm,
                        id_adm,
                        consultor,
                        nome_cliente,
                        grupo,
                        cota,
                        observacao_boleto,
                        pode_unificar,
                        aba,
                    ))
                    total_enfileiradas += 1

                # atualiza status na planilha só no modo normal
                if not modo_reexecucao:
                    log_info(
                        caminho_log=log_txt_path,
                        etapa="LEITOR_PLANILHA",
                        id_dado=id_fila_adm,
                        acao="Atualizar planilha",
                        detalhe=f"aba={aba} qtd_linhas={len(linhas_para_atualizar)}"
                    )

                    atualizar_boleto_em_lote(
                        service=service,
                        spreadsheet_id=spreadsheet_id,
                        aba=aba,
                        letra_col_boleto=letra_boleto,
                        linhas=linhas_para_atualizar
                    )

            except Exception as e:
                total_abas_puladas += 1
                msg = f"erro na aba={aba}: {str(e)}"
                log.exception(msg)

                log_erro(
                    caminho_log=log_txt_path,
                    etapa="LEITOR_PLANILHA",
                    id_dado=id_fila_adm,
                    acao="Processar aba",
                    detalhe=msg
                )

        # =========================
        # INSERE NO BANCO
        # =========================
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
                    pode_unificar,
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
                    %s,
                    NULL,
                    NULL,
                    NOW(),
                    %s
                )
            """, itens_para_inserir)

        total = len(itens_para_inserir)

        # =========================
        # ATUALIZA TOTAL DO LOTE
        # =========================
        cur.execute("""
            UPDATE tbl_fila_adm
            SET total_cotas = %s
            WHERE id_fila_adm = %s
        """, (total, id_fila_adm))

        conn.commit()

        resumo = (
            f"id_fila_adm={id_fila_adm} "
            f"mes_ref={mes_ref} "
            f"reexecucao={modo_reexecucao} "
            f"abas={len(abas)} "
            f"abas_puladas={total_abas_puladas} "
            f"lidas={total_linhas_lidas} "
            f"invalidas={total_invalidas} "
            f"bloqueadas={total_bloqueadas} "
            f"filtradas_reexec={total_filtradas_reexec} "
            f"enfileiradas={total_enfileiradas}"
        )

        log_info(
            caminho_log=log_txt_path,
            etapa="LEITOR_PLANILHA",
            id_dado=id_fila_adm,
            acao="Resumo",
            detalhe=resumo
        )

        log_info(
            caminho_log=log_txt_path,
            etapa="LEITOR_PLANILHA",
            id_dado=id_fila_adm,
            acao="Finalizar leitura",
            detalhe=f"total_enfileiradas={total}"
        )

        return total

    except Exception as e:
        conn.rollback()

        msg = f"falha geral no leitor_planilha: {str(e)}"
        log.exception(msg)

        log_erro(
            caminho_log=log_txt_path,
            etapa="LEITOR_PLANILHA",
            id_dado=id_fila_adm,
            acao="Executar leitura",
            detalhe=msg
        )

        raise

    finally:
        cur.close()
        conn.close()