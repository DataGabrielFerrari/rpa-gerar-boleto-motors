# main.py
import sys
import os
import re
import traceback
from typing import List, Tuple, Optional
from datetime import datetime

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from lib.db import get_conn
from lib.mes_ref import decidir_mes_ref  # agora retorna DecisaoMesRef
from lib.vencimento import calcular_vencimento
from entrada.lib.leitor_planilha import ler_planilhas  # ler_planilhas(id_fila_adm, modo_reexecucao, ...)

DEFAULT_HEARTBEAT_TIMEOUT_MINUTOS = 10  # fallback
DEFAULT_AUTO_UNLOCK_MINUTOS = 5         # destravar PROCESSANDO inativo > 5 min

import getpass

def _get_usuario_windows() -> str:
    u = (os.environ.get("USERNAME") or "").strip()
    if u:
        return u
    try:
        return (getpass.getuser() or "").strip() or "DESCONHECIDO"
    except Exception:
        return "DESCONHECIDO"



def _setup_env() -> None:
    if load_dotenv:
        load_dotenv()


def _setup_logging():
    import logging

    logger = logging.getLogger("orquestrador")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        h_console = logging.StreamHandler(sys.stdout)
        h_console.setFormatter(fmt)
        logger.addHandler(h_console)

    return logger


def _get_param_int(conn, nome: str, default: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT valor FROM tbl_parametros WHERE nome = %s LIMIT 1", (nome,))
    row = cur.fetchone()
    cur.close()

    if not row or row[0] is None:
        return default

    try:
        return int(str(row[0]).strip())
    except Exception:
        return default


def _sanitize_folder_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r'[\\/:*?"<>|]', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else "SEM_NOME"


def _get_lotes_root() -> str:
    # ...\Orquestrador Entrada\main.py  ->  ...\Lotes\
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "Lotes"))


def _append_log(log_txt_path: Optional[str], level: str, msg: str) -> None:
    if not log_txt_path:
        return
    try:
        os.makedirs(os.path.dirname(log_txt_path), exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} | {level.upper()} | {msg}\n"
        with open(log_txt_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _criar_estrutura_lote(nome_adm: str, id_adm: int, id_fila_adm: int) -> dict:
    lotes_root = _get_lotes_root()
    os.makedirs(lotes_root, exist_ok=True)

    pasta_adm = f"{_sanitize_folder_name(nome_adm)}_{id_adm}"
    pasta_fila = f"fila_{id_fila_adm}"

    lote_dir = os.path.join(lotes_root, pasta_adm, pasta_fila)
    boletos_dir = os.path.join(lote_dir, "Boletos")
    log_dir = os.path.join(lote_dir, "Log")

    os.makedirs(boletos_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    log_txt_path = os.path.join(log_dir, "log.txt")
    if not os.path.exists(log_txt_path):
        with open(log_txt_path, "w", encoding="utf-8") as f:
            f.write("")

    return {
        "lote_dir": lote_dir,
        "boletos_dir": boletos_dir,
        "log_dir": log_dir,
        "log_txt_path": log_txt_path,

    }


def _buscar_lotes_ativos(conn) -> List[Tuple[int, int, int, str, Optional[str], Optional[str]]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id_fila_adm, id_adm, mes_ref, status, caminho_lote, caminho_log
        FROM tbl_fila_adm
        WHERE TRIM(UPPER(status)) IN ('PENDENTE', 'PROCESSANDO')
        ORDER BY
            CASE WHEN TRIM(UPPER(status)) = 'PROCESSANDO' THEN 0 ELSE 1 END,
            id_fila_adm DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def _destravar_processando_inativos(conn, minutos: int) -> List[Tuple[int, int, int, str, Optional[str]]]:
    cur = conn.cursor()
    cur.execute(
        """
        WITH travados AS (
          SELECT f.id_fila_adm
          FROM tbl_fila_adm f
          WHERE TRIM(UPPER(f.status)) = 'PROCESSANDO'
            AND COALESCE(f.ultima_atividade, f.data_inicio, f.criado_em) < (NOW() - (%s || ' minutes')::interval)
          FOR UPDATE
        )
        UPDATE tbl_fila_adm f
        SET status = 'PENDENTE',
            maquina = NULL,
            data_inicio = NULL,
            ultima_atividade = NOW()
        FROM travados t
        WHERE f.id_fila_adm = t.id_fila_adm
        RETURNING f.id_fila_adm, f.id_adm, f.mes_ref, f.status, f.caminho_log
        """,
        (minutos,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def criar_lotes_e_enfileirar(logger) -> int:
    conn = get_conn()
    cur = conn.cursor()
    try:
        # ATUALIZADO: campos novos do tbl_adm
        cur.execute(
            """
            SELECT id_adm, nome, ultimo_mes_ref, reexecucao, mes_ref_alvo
            FROM tbl_adm
            WHERE ativo = TRUE
            ORDER BY id_adm
            """
        )
        adms = cur.fetchall()
        lotes_criados = 0

        for (id_adm, nome_adm, ultimo_mes_ref, reexecucao, mes_ref_alvo) in adms:
            # decisão nova (manual por alvo)
            decisao = decidir_mes_ref(
                mes_ref_alvo=mes_ref_alvo,
                ultimo_mes_ref=ultimo_mes_ref,
                reexecucao=bool(reexecucao),
            )

            if not decisao.pode_criar_lote or decisao.mes_ref is None:
                logger.info(
                    f"[SKIP ADM] id_adm={id_adm} mes_ref_alvo={mes_ref_alvo} "
                    f"ultimo_mes_ref={ultimo_mes_ref} reexecucao={reexecucao}"
                )
                continue

            mes_ref = decisao.mes_ref
            modo_reexecucao = decisao.modo_reexecucao
            data_vencimento = calcular_vencimento(mes_ref)

            # não duplicar lote pendente/processando para o mesmo ADM+mes_ref
            cur.execute(
                """
                SELECT 1
                FROM tbl_fila_adm
                WHERE id_adm = %s
                AND mes_ref = %s
                AND TRIM(UPPER(status)) IN ('PENDENTE', 'PROCESSANDO')
                LIMIT 1
                """,
                (id_adm, mes_ref),
            )
            if cur.fetchone():
                logger.info(f"[SKIP DUPLICADO] id_adm={id_adm} mes_ref={mes_ref}")
                continue

            usuario_maquina = _get_usuario_windows()  # ex: "gabri"

            # cria lote
            cur.execute(
                """
                INSERT INTO tbl_fila_adm (id_adm, mes_ref, status, data_vencimento, criado_em, maquina)
                VALUES (%s, %s, 'PENDENTE', %s, NOW(), %s)
                RETURNING id_fila_adm
                """,
                (id_adm, mes_ref, data_vencimento, usuario_maquina),
            )
            id_fila_adm = cur.fetchone()[0]
            conn.commit()
            lotes_criados += 1

            # cria pastas + log.txt
            paths = _criar_estrutura_lote(nome_adm=nome_adm, id_adm=id_adm, id_fila_adm=id_fila_adm)

            # grava caminhos do lote no DB
            cur.execute(
                """
                UPDATE tbl_fila_adm
                SET caminho_lote = %s,
                    caminho_log  = %s
                WHERE id_fila_adm = %s
                """,
                (paths["lote_dir"], paths["log_txt_path"], id_fila_adm),
            )
            conn.commit()

            # log local do lote
            _append_log(paths["log_txt_path"], "INFO", f"[PY] LOTE CRIADO id_fila_adm={id_fila_adm} id_adm={id_adm}")
            _append_log(paths["log_txt_path"], "INFO", f"[PY] mes_ref={mes_ref} vencimento={data_vencimento} modo_reexecucao={modo_reexecucao}")

            logger.info(
                f"[LOTE CRIADO] id_fila_adm={id_fila_adm} id_adm={id_adm} mes_ref={mes_ref} "
                f"vencimento={data_vencimento} modo_reexecucao={modo_reexecucao}"
            )
            logger.info(f"[LOTE DIR] {paths['lote_dir']}")

            # enfileira (leitor_planilha deve respeitar modo_reexecucao)
            total = ler_planilhas(
                id_fila_adm,
                modo_reexecucao,
                log_txt_path=paths["log_txt_path"],
            )
            _append_log(paths["log_txt_path"], "INFO", f"[PY] ENFILEIRAMENTO concluído: itens={total}")
            logger.info(f"[ENFILEIRAMENTO] id_fila_adm={id_fila_adm} itens={total}")

            # IMPORTANTE:
            # Aqui você escolhe quando "consumir" o alvo e atualizar ultimo_mes_ref.
            # Pelo seu modelo, isso deveria acontecer NO FINAL do lote (orquestrador de saída).
            #
            # Porém, se você quer manter o comportamento antigo (marcar aqui),
            # mantenho com um comentário e uma opção.
            #
            # ✅ Recomendado (mais seguro): NÃO atualizar tbl_adm aqui.
            # - Deixe o ciclo ser marcado quando o PAD/worker finalizar e você setar status=FINALIZADO.
            #
            # Se você insistir em marcar aqui, use este bloco:

            # if not modo_reexecucao:
            #     cur.execute(
            #         """
            #         UPDATE tbl_adm
            #         SET ultimo_mes_ref = %s,
            #             reexecucao = FALSE,
            #             mes_ref_alvo = NULL
            #         WHERE id_adm = %s
            #         """,
            #         (mes_ref, id_adm),
            #     )
            # else:
            #     cur.execute(
            #         """
            #         UPDATE tbl_adm
            #         SET reexecucao = FALSE
            #         WHERE id_adm = %s
            #         """,
            #         (id_adm,),
            #     )
            # conn.commit()

            _append_log(paths["log_txt_path"], "INFO", "[PY] Lote pronto para o Power Automate iniciar.")

        cur.close()
        conn.close()
        return lotes_criados
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass
    

def main() -> int:
    _setup_env()
    logger = _setup_logging()
    logger.info("=== Orquestrador iniciado ===")

    conn = get_conn()
    try:
        heartbeat_timeout = _get_param_int(conn, "heartbeat_timeout_minutos", DEFAULT_HEARTBEAT_TIMEOUT_MINUTOS)
        auto_unlock_min = _get_param_int(conn, "auto_unlock_minutos", DEFAULT_AUTO_UNLOCK_MINUTOS)

        # 1) AUTO-UNLOCK
        destravados = _destravar_processando_inativos(conn, auto_unlock_min)
        if destravados:
            conn.commit()
            logger.warning(f"[AUTO-UNLOCK] {len(destravados)} lote(s) destravado(s) por inatividade > {auto_unlock_min} min.")
            for (id_fila_adm, id_adm, mes_ref, status, caminho_log) in destravados:
                msg = f"[AUTO-UNLOCK OK] id_fila_adm={id_fila_adm} id_adm={id_adm} mes_ref={mes_ref} novo_status={status}"
                logger.warning(msg)
                _append_log(caminho_log, "WARNING", f"[PY] {msg}")
        logger.info(f"[PARAM] heartbeat_timeout_minutos={heartbeat_timeout} | auto_unlock_minutos={auto_unlock_min}")

        # 2) Evitar concorrência: se existe lote ativo, não cria novos
        ativos = _buscar_lotes_ativos(conn)
        if ativos:
            logger.warning(f"[ORQUESTRADOR ENCERRADO] Existem {len(ativos)} lote(s) ativo(s) (PENDENTE/PROCESSANDO).")
            for (id_fila_adm, id_adm, mes_ref, status, caminho_lote, caminho_log) in ativos:
                msg = f"[LOTE_JA_EXISTE] id_fila_adm={id_fila_adm} id_adm={id_adm} mes_ref={mes_ref} status={status}"
                logger.warning(msg)
                _append_log(caminho_log, "WARNING", f"[PY] {msg} | Orquestrador não criou novos lotes.")
            return 0

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error(f"Falha no orquestrador (pré-criação): {e}")
        logger.error(traceback.format_exc())
        return 1
    finally:
        conn.close()

    # 3) Criar lotes/enfileirar
    try:
        lotes = criar_lotes_e_enfileirar(logger)
        if lotes == 0:
            logger.info("Nenhum ADM elegível. Encerrando.")
            return 0

        logger.info(f"Orquestrador finalizado com sucesso. Lotes criados: {lotes}")
        return 0

    except Exception as e:
        logger.error(f"Falha no orquestrador: {e}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())