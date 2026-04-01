import sys
import os
import re
import traceback
import getpass
from typing import List, Tuple, Optional
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None
SRC_DIR = os.path.dirname(os.path.dirname(__file__))
ROOT_DIR = os.path.dirname(SRC_DIR)
sys.path.insert(0, SRC_DIR)

# Carrega o .env da raiz do projeto
ENV_PATH = os.path.join(ROOT_DIR, ".env")
load_dotenv(ENV_PATH, override=True)


from lib.db import get_conn
from lib.mes_ref import decidir_mes_ref
from lib.vencimento import calcular_vencimento
from shared.log import log_info, log_erro
from lib.leitor_planilha import ler_planilhas


DEFAULT_HEARTBEAT_TIMEOUT_MINUTOS = 10
DEFAULT_AUTO_UNLOCK_MINUTOS = 5


def get_usuario_windows() -> str:
    usuario = (os.environ.get("USERNAME") or "").strip()
    if usuario:
        return usuario
    try:
        return (getpass.getuser() or "").strip() or "DESCONHECIDO"
    except Exception:
        return "DESCONHECIDO"


def setup_env() -> None:
    if load_dotenv:
        load_dotenv()


def setup_logging():
    import logging

    logger = logging.getLogger("orquestrador")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def get_param_int(conn, nome: str, valor_padrao: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute("SELECT valor FROM tbl_parametros WHERE nome = %s LIMIT 1", (nome,))
        row = cur.fetchone()

        if not row or row[0] is None:
            return valor_padrao

        return int(str(row[0]).strip())
    except Exception:
        return valor_padrao
    finally:
        cur.close()


def sanitize_folder_name(nome: str) -> str:
    nome = (nome or "").strip()
    nome = re.sub(r'[\\/:*?"<>|]', "_", nome)
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome or "SEM_NOME"


def get_lotes_root() -> str:
    pasta_atual = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(pasta_atual, "..", "..", "Lotes"))

def criar_estrutura_lote(nome_adm: str, id_adm: int, id_fila_adm: int) -> dict:
    lotes_root = get_lotes_root()
    os.makedirs(lotes_root, exist_ok=True)

    pasta_adm = f"{sanitize_folder_name(nome_adm)}_{id_adm}"
    pasta_fila = f"fila_{id_fila_adm}"

    lote_dir = os.path.join(lotes_root, pasta_adm, pasta_fila)
    boletos_dir = os.path.join(lote_dir, "Boletos")
    log_dir = os.path.join(lote_dir, "Log")
    log_txt_path = os.path.join(log_dir, "log.txt")

    os.makedirs(boletos_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    if not os.path.exists(log_txt_path):
        with open(log_txt_path, "w", encoding="utf-8") as arquivo:
            arquivo.write("")

    return {
        "lote_dir": lote_dir,
        "boletos_dir": boletos_dir,
        "log_dir": log_dir,
        "log_txt_path": log_txt_path,
    }


def buscar_lotes_ativos(conn) -> List[Tuple[int, int, int, str, Optional[str], Optional[str]]]:
    cur = conn.cursor()
    try:
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
        return cur.fetchall()
    finally:
        cur.close()


def destravar_processando_inativos(conn, minutos: int) -> List[Tuple[int, int, int, str, Optional[str]]]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH travados AS (
              SELECT f.id_fila_adm
              FROM tbl_fila_adm f
              WHERE TRIM(UPPER(f.status)) = 'PROCESSANDO'
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
        return cur.fetchall()
    finally:
        cur.close()


def criar_lotes_e_enfileirar(logger) -> int:
    conn = get_conn()
    cur = conn.cursor()

    try:
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

        for id_adm, nome_adm, ultimo_mes_ref, reexecucao, mes_ref_alvo in adms:
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

            usuario_maquina = get_usuario_windows()

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

            paths = criar_estrutura_lote(nome_adm, id_adm, id_fila_adm)

            cur.execute(
                """
                UPDATE tbl_fila_adm
                SET caminho_lote = %s,
                    caminho_log = %s
                WHERE id_fila_adm = %s
                """,
                (paths["lote_dir"], paths["log_txt_path"], id_fila_adm),
            )
            conn.commit()

            log_info(
                paths["log_txt_path"],
                etapa="CRIAR_LOTE",
                id_dado=id_fila_adm,
                acao="Criar lote",
                detalhe=f"id_adm={id_adm}",
            )

            log_info(
                paths["log_txt_path"],
                etapa="DEFINIR_MES_REF",
                id_dado=id_fila_adm,
                acao="Definir parâmetros do lote",
                detalhe=f"mes_ref={mes_ref} vencimento={data_vencimento} modo_reexecucao={modo_reexecucao}",
            )

            logger.info(
                f"[LOTE CRIADO] id_fila_adm={id_fila_adm} id_adm={id_adm} "
                f"mes_ref={mes_ref} vencimento={data_vencimento} modo_reexecucao={modo_reexecucao}"
            )
            logger.info(f"[LOTE DIR] {paths['lote_dir']}")

            total = ler_planilhas(
                id_fila_adm,
                modo_reexecucao,
                log_txt_path=paths["log_txt_path"],
            )

            log_info(
                paths["log_txt_path"],
                etapa="ENFILEIRAR",
                id_dado=id_fila_adm,
                acao="Enfileirar itens",
                detalhe=f"itens={total}",
            )

            logger.info(f"[ENFILEIRAMENTO] id_fila_adm={id_fila_adm} itens={total}")

            log_info(
                paths["log_txt_path"],
                etapa="FINALIZAR_PREPARO",
                id_dado=id_fila_adm,
                acao="Preparar lote",
                detalhe="Lote pronto para o Power Automate iniciar",
            )

        return lotes_criados

    finally:
        try:
            cur.close()
        except Exception:
            pass

        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    setup_env()
    logger = setup_logging()
    logger.info("=== Orquestrador iniciado ===")

    conn = get_conn()

    try:
        heartbeat_timeout = get_param_int(
            conn,
            "heartbeat_timeout_minutos",
            DEFAULT_HEARTBEAT_TIMEOUT_MINUTOS,
        )
        auto_unlock_min = get_param_int(
            conn,
            "auto_unlock_minutos",
            DEFAULT_AUTO_UNLOCK_MINUTOS,
        )

        destravados = destravar_processando_inativos(conn, auto_unlock_min)
        if destravados:
            conn.commit()
            logger.warning(
                f"[AUTO-UNLOCK] {len(destravados)} lote(s) destravado(s) por inatividade > {auto_unlock_min} min."
            )

            for id_fila_adm, id_adm, mes_ref, status, caminho_log in destravados:
                msg = (
                    f"id_fila_adm={id_fila_adm} id_adm={id_adm} "
                    f"mes_ref={mes_ref} novo_status={status}"
                )
                logger.warning(f"[AUTO-UNLOCK OK] {msg}")


        logger.info(
            f"[PARAM] heartbeat_timeout_minutos={heartbeat_timeout} | "
            f"auto_unlock_minutos={auto_unlock_min}"
        )

        ativos = buscar_lotes_ativos(conn)
        if ativos:
            logger.warning(
                f"[ORQUESTRADOR ENCERRADO] Existem {len(ativos)} lote(s) ativo(s) (PENDENTE/PROCESSANDO)."
            )

            for id_fila_adm, id_adm, mes_ref, status, caminho_lote, caminho_log in ativos:
                msg = f"id_adm={id_adm} mes_ref={mes_ref} status={status}"
                logger.warning(f"[LOTE_JA_EXISTE] id_fila_adm={id_fila_adm} {msg}")

            return 0

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        logger.error(f"Falha no orquestrador (pré-criação): {e}")
        logger.error(traceback.format_exc())

        log_erro(
            None,
            etapa="PRE_CRIAÇÃO",
            id_dado=None,
            acao="Executar validações iniciais",
            detalhe=str(e),
        )

        return 1

    finally:
        conn.close()

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

        log_erro(
            None,
            etapa="CRIAR_LOTES",
            id_dado=None,
            acao="Criar lotes e enfileirar",
            detalhe=str(e),
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())