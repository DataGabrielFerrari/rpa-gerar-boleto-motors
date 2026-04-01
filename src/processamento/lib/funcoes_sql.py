import os
from datetime import datetime
from processamento.lib.db import get_conn
import sys

# Atribuir parametros da tbl_fila_cotas com base no id_cota recebido de entrada
def obter_fila():
    print("Python exe:", sys.executable, flush=True)
    print("Diretório atual:", os.getcwd(), flush=True)
    print("Argumentos recebidos:", sys.argv, flush=True)

    if len(sys.argv) < 2:
        print("ERRO: parâmetros não recebidos", flush=True)
        sys.exit(1)

    id_cota = int(sys.argv[1])
    print(f"id_cota recebido: {id_cota}", flush=True)

    with get_conn() as conexao:
        with conexao.cursor() as cur:
            cur.execute(
                """
                SELECT nome_cliente, grupo, cota, nome_consultor, pode_unificar
                FROM tbl_fila_cotas
                WHERE id_cota = %s
                """,
                (id_cota,)
            )
            row = cur.fetchone()

        if not row:
            raise ValueError(f"id_Cota: {id_cota} não identificado no banco de dados")

        with conexao.cursor() as cur2:
            cur2.execute(
                """
                SELECT a.id_fila_adm, a.data_vencimento, a.caminho_lote, a.caminho_log
                FROM tbl_fila_adm a
                JOIN tbl_fila_cotas c ON a.id_adm = c.id_adm
                WHERE a.id_fila_adm = (
                    SELECT id_fila_adm
                    FROM tbl_fila_cotas
                    WHERE id_cota = %s
                )
                LIMIT 1
                """,
                (id_cota,)
            )
            row_data = cur2.fetchone()

        if not row_data:
            raise ValueError("Data de vencimento não encontrada")

    return id_cota, row, row_data


def executar_update(query: str, params: tuple = ()) -> int:
    """
    Executa UPDATE/INSERT/DELETE no banco.
    Retorna a quantidade de linhas afetadas.
    """
    with get_conn() as conexao:
        with conexao.cursor() as cur:
            cur.execute(query, params)
            linhas_afetadas = cur.rowcount
        conexao.commit()
    return linhas_afetadas

# atualizar status erro
def inserir_cotas_nao_encontradas(
    id_fila:int,
    nome_cliente: str,
    grupo: int,
    cota: int
) -> int:
    """
    Insere uma nova cota na tbl_fila_cotas.
    """
    query = """
    INSERT INTO tbl_cotas_nao_encontradas(
    id_fila_adm,
    nome_cliente,
    grupo,cota)
    VALUES (%s,%s,%s,%s)
    """

    params = (
        id_fila,
        nome_cliente,
        grupo,
        cota
    )

    return executar_update(query, params)

# atualizar status erro
def atualizar_status_erro(
    id_cota: int,
    status: str,
    observacao: str = None,
    caminho_print: str = None
) -> int:
    """
    Atualiza status de uma cota na tbl_fila_cotas.
    """
    query = """
        UPDATE tbl_fila_cotas
        SET status = %s,
            observacao = %s,
            caminho_print = %s
        WHERE id_cota = %s
    """
    params = (
        status,
        observacao,
        caminho_print,
        id_cota
    )
    return executar_update(query, params)


def atualizar_caminho_boleto(
    id_cota: int,
    caminho_boleto: str
) -> int:
    """
    Atualiza caminho do boleto na tbl_fila_cotas.
    """
    query = """
        UPDATE tbl_fila_cotas
        SET caminho_boleto = %s
        WHERE id_cota = %s
    """
    params = (
        caminho_boleto,
        id_cota
    )
    return executar_update(query, params)


def atualizar_status(
    id_cota: int,
    status: str,
    observacao: str
) -> int:
    """
    Atualiza status de uma cota na tbl_fila_cotas.
    """
    query = """
        UPDATE tbl_fila_cotas
        SET status = %s, observacao = %s
        WHERE id_cota = %s
    """
    params = (
        status,
        observacao,
        id_cota
    )
    return executar_update(query, params)

def atualizar_contador_status(
    id_fila_adm: int,
    status: str,
) -> int:
    """
    Incrementa apenas a coluna correspondente ao status.
    """

    status_normalizado = status.upper().strip()

    mapa_status = {
        "NORMAL": "clientes_sucesso",
        "UNIFICADO": "clientes_sucesso",
        "ERRO": "clientes_erro",
        "FALHA": "clientes_erro",
        "EM ATRASO": "clientes_com_atraso",
        "ADIANTADO": "clientes_adiantados"
    }

    coluna_contador = mapa_status.get(status_normalizado)

    if not coluna_contador:
        return 0

    query = f"""
        UPDATE tbl_fila_adm
        SET {coluna_contador} = COALESCE({coluna_contador}, 0) + 1
        WHERE id_fila_adm = %s
    """

    params = (id_fila_adm,)

    return executar_update(query, params)


def verificar_cota_existe_na_fila(
    id_fila_adm: int,
    grupo: str,
    cota: str
) -> bool:
    grupo = str(grupo).zfill(6)
    cota = str(cota).zfill(4)
    with get_conn() as conexao:
        with conexao.cursor() as cur:

            cur.execute(
                """
                SELECT 1
                FROM tbl_fila_cotas
                WHERE id_fila_adm = %s
                AND grupo = %s
                AND cota = %s
                LIMIT 1
                """,
                (id_fila_adm, grupo, cota)
            )

            return cur.fetchone() is not None
        
def atualizar_status_unificados(
    id_fila_adm:int,
    grupo: str,
    cota: str,
    status: str,
    observacao: str,
) -> int:
    """
    Atualiza status de uma cota na tbl_fila_cotas.
    """
    query = """
        UPDATE tbl_fila_cotas
        SET status = %s, observacao = %s
        WHERE id_fila_adm = %s AND grupo = %s AND cota = %s
    """
    params = (
        status,
        observacao,
        id_fila_adm,
        grupo,
        cota
    )
    return executar_update(query, params)


def obter_url_newcon():
    with get_conn() as conexao:
        with conexao.cursor() as cur:
            cur.execute(
                """
                SELECT valor
                FROM tbl_parametros
                WHERE nome = 'url_newcon'
                """
            )
            row = cur.fetchone()

    return row[0] if row else None