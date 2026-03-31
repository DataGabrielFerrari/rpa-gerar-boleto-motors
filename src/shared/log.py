import os
from datetime import datetime
from typing import Optional
import inspect
import os

def obter_origem():
    frame = inspect.stack()[2]

    caminho_completo = frame.filename

    # deixa caminho relativo a src
    if "src" in caminho_completo:
        caminho_relativo = caminho_completo.split("src")[-1]
    else:
        caminho_relativo = os.path.basename(caminho_completo)

    caminho_relativo = caminho_relativo.replace("\\", "/").lstrip("/")

    linha = frame.lineno

    return f"{caminho_relativo}:{linha}"

# ---------- FUNÇÕES BASE ----------

def obter_data_hora() -> str:
    """Retorna data e hora atual formatada."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def criar_pasta_se_nao_existir(caminho: str) -> None:
    """Cria a pasta caso não exista."""
    if caminho:
        os.makedirs(os.path.dirname(caminho), exist_ok=True)


def formatar_linha_log(
    nivel: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    status: str,
    detalhe: str = ""
) -> str:
    """Monta a linha padrão do log."""

    data = obter_data_hora()
    origem = obter_origem()

    return (
        f"{data} | "
        f"{nivel.upper()} | "
        f"{etapa} | "
        f"{id_dado if id_dado else '-'} | "
        f"{acao} | "
        f"{status} | "
        f"{origem} | "
        f"{detalhe}"
    )


def escrever_log(
    caminho_log: str,
    linha: str
) -> None:
    """Escreve uma linha no arquivo de log."""
    try:
        criar_pasta_se_nao_existir(caminho_log)

        with open(caminho_log, "a", encoding="utf-8") as arquivo:
            arquivo.write(linha + "\n")

    except Exception:
        pass


# ---------- FUNÇÃO PRINCIPAL ----------

def registrar_log(
    caminho_log: str,
    nivel: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    status: str,
    detalhe: str = "",
    tempo_ms: Optional[int] = None
) -> None:
    """
    Função principal de log.
    Use esta função no projeto.
    """

    linha = formatar_linha_log(
        nivel=nivel,
        etapa=etapa,
        id_dado=id_dado,
        acao=acao,
        status=status,
        detalhe=detalhe,
        tempo_ms=tempo_ms
    )

    escrever_log(caminho_log, linha)


# ---------- FUNÇÕES DE NÍVEL ----------

def log_info(
    caminho_log: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    detalhe: str = "",
    tempo_ms: Optional[int] = None
) -> None:

    registrar_log(
        caminho_log,
        nivel="INFO",
        etapa=etapa,
        id_dado=id_dado,
        acao=acao,
        status="SUCESSO",
        detalhe=detalhe,
        tempo_ms=tempo_ms
    )


def log_erro(
    caminho_log: str,
    etapa: str,
    id_dado: Optional[int],
    acao: str,
    detalhe: str = "",
    tempo_ms: Optional[int] = None
) -> None:

    registrar_log(
        caminho_log,
        nivel="ERROR",
        etapa=etapa,
        id_dado=id_dado,
        acao=acao,
        status="FALHA",
        detalhe=detalhe,
        tempo_ms=tempo_ms
    )


