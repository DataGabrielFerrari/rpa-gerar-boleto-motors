import os
from datetime import datetime

from saida.lib.db import get_conn
from saida.lib.drive_service import processar_drive_finalizados
from saida.jobs.enviar_email import enviar_email_lote
from saida.jobs.atualizar_planilha import atualizar_planilhas_finalizadas


def append_log(caminho_log: str, nivel: str, mensagem: str):
    try:
        os.makedirs(os.path.dirname(caminho_log), exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(caminho_log, "a", encoding="utf-8") as f:
            f.write(f"{ts} | {nivel.upper()} | {mensagem}\n")
    except Exception:
        pass


class FileLogger:
    def __init__(self, caminho_log: str):
        self.caminho_log = caminho_log

    def info(self, msg: str):
        append_log(self.caminho_log, "INFO", msg)

    def warning(self, msg: str):
        append_log(self.caminho_log, "WARNING", msg)

    def error(self, msg: str):
        append_log(self.caminho_log, "ERROR", msg)


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
        # Puxa id_adm e mes_ref (coluna que existe)
        cur.execute("""
            SELECT id_adm, mes_ref
            FROM tbl_fila_adm
            WHERE id_fila_adm = %s
        """, (id_fila_adm,))
        row = cur.fetchone()

        if not row:
            raise RuntimeError(f"Lote {id_fila_adm} não encontrado em tbl_fila_adm")

        id_adm, mes_ref = row
        if mes_ref is None:
            raise RuntimeError(f"Lote {id_fila_adm} sem mes_ref (não dá pra atualizar ultimo_mes_ref)")

        # Atualiza a trava do ADM (coluna que existe)
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
    print("=== ORQUESTRADOR SAÍDA INICIADO ===")
    conn = get_conn()

    try:
        lotes = buscar_lotes(conn)
        if not lotes:
            print("Nenhum lote FINALIZADO.")
            return

        for id_fila_adm, caminho_log in lotes:
            flog = FileLogger(caminho_log)
            erro = False

            flog.info(f"--- Iniciando lote {id_fila_adm} ---")

            try:
                flog.info("Iniciando Drive...")
                processar_drive_finalizados(flog)

                flog.info("Enviando Email...")
                enviar_email_lote(id_fila_adm, flog)

                flog.info("Atualizando Planilha...")
                atualizar_planilhas_finalizadas(flog)


            except Exception as e:
                erro = True
                flog.error(f"Erro no lote {id_fila_adm}: {e}")

            if not erro:
                atualizar_ultima_execucao(conn, id_fila_adm)  # ✅ aqui
                marcar_encerrado(conn, id_fila_adm)
                flog.info(f"Lote {id_fila_adm} ENCERRADO com sucesso.")
            else:
                flog.error(f"Lote {id_fila_adm} NÃO ENCERRADO devido erro.")

            flog.info(f"--- Fim lote {id_fila_adm} ---")

    finally:
        conn.close()

    print("=== ORQUESTRADOR SAÍDA FINALIZADO ===")


if __name__ == "__main__":
    main()