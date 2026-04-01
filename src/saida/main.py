import os
import sys
from dotenv import load_dotenv

CURRENT_DIR = os.path.dirname(__file__)          # src/saida
SRC_DIR = os.path.dirname(CURRENT_DIR)           # src
ROOT_DIR = os.path.dirname(SRC_DIR)              # projeto

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

ENV_PATH = os.path.join(ROOT_DIR, ".env")
load_dotenv(ENV_PATH, override=True)

from saida.lib.db import get_conn
from saida.lib.drive_service import processar_drive_finalizados
from saida.jobs.enviar_email import enviar_email_lote
from saida.jobs.atualizar_planilha import atualizar_planilhas_finalizadas

from shared.log import log_info, log_erro


def buscar_lotes(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id_fila_adm, caminho_log
            FROM tbl_fila_adm
            WHERE TRIM(UPPER(status)) = 'FINALIZADO'
            ORDER BY id_fila_adm
        """)
        return cur.fetchall()


def atualizar_ultima_execucao(conn, id_fila_adm: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id_adm, mes_ref
            FROM tbl_fila_adm
            WHERE id_fila_adm = %s
        """, (id_fila_adm,))

        row = cur.fetchone()

        if not row:
            raise RuntimeError(f"Lote {id_fila_adm} não encontrado")

        id_adm, mes_ref = row

        if mes_ref is None:
            raise RuntimeError(f"Lote {id_fila_adm} sem mes_ref")

        cur.execute("""
            UPDATE tbl_adm
            SET ultimo_mes_ref = %s
            WHERE id_adm = %s
        """, (mes_ref, id_adm))

    conn.commit()


def marcar_encerrado(conn, id_fila_adm):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE tbl_fila_adm
            SET status = 'ENCERRADO'
            WHERE id_fila_adm = %s
        """, (id_fila_adm,))
    conn.commit()


def main():
    conn = get_conn()

    try:
        lotes = buscar_lotes(conn)

        if not lotes:
            return

        for id_fila_adm, caminho_log in lotes:
            erro = False

            log_info(
                caminho_log=caminho_log,
                etapa="SAIDA",
                id_dado=id_fila_adm,
                acao="Iniciar lote",
                detalhe=f"id_fila_adm={id_fila_adm}"
            )

            try:
                log_info(
                    caminho_log=caminho_log,
                    etapa="SAIDA",
                    id_dado=id_fila_adm,
                    acao="Iniciar Drive",
                    detalhe="Processando etapa Drive"
                )
                processar_drive_finalizados(id_fila_adm)

                log_info(
                    caminho_log=caminho_log,
                    etapa="SAIDA",
                    id_dado=id_fila_adm,
                    acao="Enviar email",
                    detalhe="Processando envio de email"
                )
                enviar_email_lote(id_fila_adm)

                log_info(
                    caminho_log=caminho_log,
                    etapa="SAIDA",
                    id_dado=id_fila_adm,
                    acao="Atualizar planilha",
                    detalhe="Processando atualização da planilha"
                )
                atualizar_planilhas_finalizadas(id_fila_adm)

            except Exception as e:
                erro = True
                log_erro(
                    caminho_log=caminho_log,
                    etapa="SAIDA",
                    id_dado=id_fila_adm,
                    acao="Executar saída",
                    detalhe=f"Erro no lote {id_fila_adm}: {e}"
                )

            if not erro:
                try:
                    atualizar_ultima_execucao(conn, id_fila_adm)
                    marcar_encerrado(conn, id_fila_adm)

                    log_info(
                        caminho_log=caminho_log,
                        etapa="SAIDA",
                        id_dado=id_fila_adm,
                        acao="Finalizar lote",
                        detalhe=f"Lote {id_fila_adm} encerrado com sucesso"
                    )

                except Exception as e:
                    log_erro(
                        caminho_log=caminho_log,
                        etapa="SAIDA",
                        id_dado=id_fila_adm,
                        acao="Encerrar lote",
                        detalhe=f"Falha ao encerrar lote {id_fila_adm}: {e}"
                    )
            else:
                log_erro(
                    caminho_log=caminho_log,
                    etapa="SAIDA",
                    id_dado=id_fila_adm,
                    acao="Lote não encerrado",
                    detalhe=f"Lote {id_fila_adm} não encerrado devido erro"
                )

    finally:
        conn.close()


if __name__ == "__main__":
    main()