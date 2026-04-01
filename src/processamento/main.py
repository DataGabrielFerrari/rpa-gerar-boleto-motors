import sys
import os
from dotenv import load_dotenv
from time import sleep


# Adiciona /src no path para permitir imports absolutos do projeto
SRC_DIR = os.path.dirname(os.path.dirname(__file__))
ROOT_DIR = os.path.dirname(SRC_DIR)
sys.path.insert(0, SRC_DIR)

# Carrega o .env da raiz do projeto
ENV_PATH = os.path.join(ROOT_DIR, ".env")
load_dotenv(ENV_PATH, override=True)

from playwright.sync_api import sync_playwright

from jobs.worker import (
    carregar_contexto_fila,
    conectar_aba_newcon,
    pesquisar_cota,
    tratar_erro_localizacao,
    abrir_formulario_boletos,
    preparar_filtros_boletos,
    analisar_tabela_boletos,
    processar_resultado_boletos,
    decidir_status_final,
    finalizar_processamento,
    clicar_seguro,
    aguardar_resultado_pesquisa
)

from processamento.lib.funcoes_sql import obter_url_newcon


try:
    # Carrega todos os dados necessários da fila já tratados
    contexto = carregar_contexto_fila()

    id_cota = contexto["id_cota"]
    nome_cliente = contexto["nome_cliente"]
    grupo = contexto["grupo"]
    cota = contexto["cota"]
    pode_unificar = contexto["pode_unificar"]
    id_fila_adm = int(contexto["id_fila_adm"])
    caminho_falha = contexto["caminho_falha"]
    caminho_lote = contexto["caminho_lote"]
    nome_consultor = contexto["nome_consultor"]
    data_vencimento_formatada = contexto["data_vencimento"]
    caminho_log = contexto["caminho_log"]

    # Busca a URL parametrizada do sistema
    url = obter_url_newcon()

    with sync_playwright() as p:
        # Conecta na aba já aberta do Newcon
        page = conectar_aba_newcon(p, url, caminho_log, id_cota)

        # Pesquisa a cota na tela inicial
        pesquisar_cota(page, grupo, cota)
        resultado_pesquisa = aguardar_resultado_pesquisa(page, timeout=15000)

        if resultado_pesquisa == "ERRO":
            if tratar_erro_localizacao(page, id_cota, id_fila_adm, grupo, cota, caminho_falha):
                sys.exit(0)
  
        # Abre o formulário de boletos e tenta marcar unificação quando permitido
        clicou_unificar = abrir_formulario_boletos(page, pode_unificar)

        # Preenche filtros da tela e captura o nome exibido pelo sistema
        nome_cliente_sistema = preparar_filtros_boletos(page, data_vencimento_formatada)

        # Analisa a tabela e devolve apenas o que realmente deve ser processado
        dados_tabela = analisar_tabela_boletos(
            page,
            data_vencimento_formatada,
            id_fila_adm
        )

        resultado = dados_tabela["resultado"]
        parcelas = dados_tabela["parcelas"]
        parcelas_atraso = dados_tabela["parcelas_atraso"]
        meses_parcelas = dados_tabela["meses_parcelas"]

        # Executa as ações por linha: marca parcelas, insere cotas não encontradas
        # e atualiza unificados quando necessário
        processar_resultado_boletos(
            page,
            resultado,
            id_fila_adm,
            nome_cliente,
            parcelas,
            clicou_unificar
        )

        # Define o status final da cota com base no resumo da tabela
        status_info = decidir_status_final(
            parcelas,
            parcelas_atraso,
            meses_parcelas,
            clicou_unificar
        )

        # Atualiza status e caminho do boleto no banco
        # Se retornar None, significa que não há boleto para emitir
        caminho_boleto = finalizar_processamento(
            id_cota,
            id_fila_adm,
            caminho_lote,
            nome_consultor,
            nome_cliente,
            nome_cliente_sistema,
            meses_parcelas,
            parcelas,
            clicou_unificar,
            status_info
        )

        # Só tenta emitir quando realmente existe boleto para gerar
        if caminho_boleto:
            clicar_seguro(page, "#ctl00_Conteudo_btnEmitir", "botão Emitir")

        sys.exit(0)

except Exception as e:
    print("ERRO NO PYTHON:", str(e), flush=True)
    sys.exit(1)