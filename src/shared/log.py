import os
import inspect
from datetime import datetime
from typing import Optional


def obter_origem() -> str:
    stack = inspect.stack()

    frame = None
    for item in stack:
        caminho = item.filename.replace("\\", "/")
        if not caminho.endswith("shared/log.py"):
            frame = item
            break

    if frame is None:
        frame = stack[1]

    caminho_completo = frame.filename.replace("\\", "/")

    if "/src/" in caminho_completo:
        caminho_relativo = caminho_completo.split("/src/", 1)[1]
    else:
        caminho_relativo = os.path.basename(caminho_completo)

    linha = frame.lineno
    return f"{caminho_relativo}:{linha}"


def obter_data_hora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def criar_pasta_se_nao_existir(caminho: str) -> None:
    if not caminho:
        raise ValueError("caminho_log vazio ou inválido")

    pasta = os.path.dirname(caminho)

    if pasta:
        os.makedirs(pasta, exist_ok=True)


def formatar_linha_log(
    nivel: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    status: str,
    detalhe: str = ""
) -> str:
    data = obter_data_hora()
    origem = obter_origem()

    return (
        f"{data} | "
        f"{nivel.upper()} | "
        f"{etapa} | "
        f"{id_dado if id_dado is not None else '-'} | "
        f"{acao} | "
        f"{status} | "
        f"{origem} | "
        f"{detalhe}"
    )


def escrever_log(caminho_log: str, linha: str) -> None:
    try:
        criar_pasta_se_nao_existir(caminho_log)

        with open(caminho_log, "a", encoding="utf-8") as arquivo:
            arquivo.write(linha + "\n")

    except Exception as e:
        print(f"[ERRO LOG] Falha ao escrever log em '{caminho_log}': {e}")
        raise


def registrar_log(
    caminho_log: str,
    nivel: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    status: str,
    detalhe: str = "",
) -> None:
    linha = formatar_linha_log(
        nivel=nivel,
        etapa=etapa,
        id_dado=id_dado,
        acao=acao,
        status=status,
        detalhe=detalhe
    )

    escrever_log(caminho_log, linha)


def log_info(
    caminho_log: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    detalhe: str = ""
) -> None:
    registrar_log(
        caminho_log=caminho_log,
        nivel="INFO",
        etapa=etapa,
        id_dado=id_dado,
        acao=acao,
        status="SUCESSO",
        detalhe=detalhe,
    )


def log_erro(
    caminho_log: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    detalhe: str = ""
) -> None:
    registrar_log(
        caminho_log=caminho_log,
        nivel="ERROR",
        etapa=etapa,
        id_dado=id_dado,
        acao=acao,
        status="FALHA",
        detalhe=detalhe,
    )