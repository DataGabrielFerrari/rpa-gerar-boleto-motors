from datetime import datetime
from pathlib import Path
import re
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from processamento.lib.funcoes_sql import (
    obter_fila,
    atualizar_status_erro,
    atualizar_status,
    inserir_cotas_nao_encontradas,
    atualizar_caminho_boleto,
    verificar_cota_existe_na_fila,
    atualizar_status_unificados,
    atualizar_contador_status
)
from shared.log import log_info, log_erro


# ============================================================
# CONTEXTO INICIAL
# ============================================================

def carregar_contexto_fila():
    """
    Busca os dados da fila no banco e devolve tudo já tratado
    no formato que o fluxo principal precisa.
    """
    id_cota, row, row_data = obter_fila()

    return {
        "id_cota": id_cota,
        "nome_cliente": str(row[0]),
        "grupo": str(row[1]).zfill(6),
        "cota": str(row[2]).zfill(4),
        "nome_consultor": str(row[3]),
        "pode_unificar": str(row[4]),
        "id_fila_adm": str(row_data[0]),
        "data_vencimento": str(row_data[1]),
        "caminho_lote": str(row_data[2]),
        "caminho_log": str(row_data[3]),
        "caminho_falha": fr"{row_data[2]}\Boletos\FALHAS",
        "data_vencimento": datetime.strptime(
            str(row_data[1]), "%Y-%m-%d"
        ).strftime("%d/%m/%Y")
    }


# ============================================================
# NAVEGAÇÃO / CONEXÃO COM O NAVEGADOR
# ============================================================

def conectar_aba_newcon(p, url, caminho_log, id_cota):
    """
    Conecta no navegador já aberto via CDP, localiza a aba do Newcon
    pela URL e devolve a página pronta para uso.
    """
    browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
    context = browser.contexts[0]

    for page in context.pages:
        if url.lower() in page.url.lower():
            page.bring_to_front()
            page.wait_for_load_state("networkidle")
            log_info(caminho_log, "PROCESSAMENTO", id_cota, "Conectar na aba:", page.url)
            return page

    raise Exception("Não achei a aba do Newcon aberta")


def pesquisar_cota(page, grupo, cota):
    """
    Acessa a tela de atendimento e pesquisa a cota desejada.
    """
    page.locator("#ctl00_img_Atendimento").click()
    page.locator("#ctl00_Conteudo_edtGrupo").fill(grupo)
    page.locator("#ctl00_Conteudo_edtCota").fill(cota)
    page.locator("#ctl00_Conteudo_btnLocalizar").click()


def abrir_formulario_boletos(page, pode_unificar):
    """
    Abre o formulário de boletos e, se permitido, marca a opção
    de unificação de parcelas.
    """
    page.locator("#ctl00_Conteudo_Menu_CONAT_grdMenu_CONAT_ctl10_hlkFormulario").click()

    clicou_unificar = False

    if pode_unificar.upper() == "SIM":
        botao_unificar = page.locator("#ctl00_Conteudo_chkUnificarParcelas")
        if botao_unificar.count() > 0:
            botao_unificar.click()
            clicou_unificar = True

    return clicou_unificar


def preparar_filtros_boletos(page, data_vencimento_formatada):
    """
    Preenche os filtros da tela de boletos com a data de referência,
    reseta a seleção de parcelas e executa a busca.
    """
    nome_cliente_sistema = page.locator(
        "#ctl00_Conteudo_Cabecalho_dados_cota_lblCD_Grupo"
    ).inner_text()

    page.locator("#ctl00_Conteudo_edtDT_Compensacao").fill(data_vencimento_formatada)
    page.locator("#ctl00_Conteudo_edtDT_Base_Pendencias").fill(data_vencimento_formatada)
    page.locator("#ctl00_Conteudo_ckSelecionar_Todas").click()
    page.locator("#ctl00_Conteudo_ckSelecionar_Todas").click()
    page.locator("#ctl00_Conteudo_btnLocalizar").click()

    return nome_cliente_sistema


def clicar_seguro(page, selector, nome="botão", timeout=10000):
    """
    Tenta clicar no elemento de forma robusta:
    1. clique normal
    2. clique forçado
    3. clique via coordenada do mouse

    Se tudo falhar, tira print e levanta erro.
    """
    botao = page.locator(selector)

    try:
        botao.wait_for(state="visible", timeout=timeout)

        if not botao.is_enabled():
            raise Exception(f"{nome} encontrado, mas está desabilitado")

        botao.scroll_into_view_if_needed()
        page.wait_for_timeout(300)

        try:
            botao.click(timeout=timeout)
            return
        except PlaywrightTimeoutError:
            pass

        page.wait_for_timeout(500)

        try:
            botao.click(timeout=timeout, force=True)
            return
        except PlaywrightTimeoutError:
            pass

        box = botao.bounding_box()
        if box:
            page.mouse.click(
                box["x"] + box["width"] / 2,
                box["y"] + box["height"] / 2
            )
            return

        raise Exception(f"Não foi possível clicar em {nome}")

    except Exception as e:
        page.screenshot(path="falha_click_emitir.png", full_page=True)
        raise Exception(f"Falha ao clicar em {nome}: {e}")


# ============================================================
# TRATAMENTO DE ERRO VISUAL NA TELA
# ============================================================
def aguardar_resultado_pesquisa(page, timeout=10000):
    """
    Espera até acontecer 1 destes 2 cenários:
    - aparecer mensagem de erro visual
    - aparecer o menu/formulário da cota
    """
    page.wait_for_timeout(500)

    locator_erro = page.locator("#ctl00_Conteudo_lblErrMsg")
    locator_formulario = page.locator("#ctl00_Conteudo_Menu_CONAT_grdMenu_CONAT_ctl10_hlkFormulario")

    fim = time.time() + (timeout / 1000)

    while time.time() < fim:
        try:
            if locator_erro.count() > 0:
                texto = locator_erro.first.inner_text().strip()
                if texto:
                    return "ERRO"
        except Exception:
            pass

        try:
            if locator_formulario.count() > 0 and locator_formulario.first.is_visible():
                return "OK"
        except Exception:
            pass

        page.wait_for_timeout(300)

    return "TIMEOUT"
def tratar_erro_localizacao(page, id_cota, id_fila_adm, grupo, cota, caminho_falha):
    """
    Verifica se a tela exibiu mensagem de erro após a pesquisa da cota.
    Se houver erro:
    - limpa o texto para usar no nome do arquivo
    - salva print da tela
    - atualiza o status da cota no banco
    """
    locator_erro = page.locator("#ctl00_Conteudo_lblErrMsg")

    if locator_erro.count() == 0:
        return False

    texto_erro = locator_erro.first.inner_text().strip()
    if not texto_erro:
        return False

    texto_limpo = re.sub(r'[\\/*?:"<>|]', "", texto_erro)[:60]

    Path(caminho_falha).mkdir(parents=True, exist_ok=True)
    nome_arquivo = f"FALHA_{grupo}_{cota}_{texto_limpo}.png"
    caminho_print = Path(caminho_falha) / nome_arquivo

    page.screenshot(path=str(caminho_print), full_page=True)
    atualizar_status_erro(id_cota, "ERRO", texto_limpo, str(caminho_print))
    atualizar_contador_status(id_fila_adm, "ERRO")

    return True


# ============================================================
# UTILITÁRIOS DA TABELA
# ============================================================

def desmarcar_se_estiver_marcado(page, id_botao):
    """
    Desmarca a linha apenas se o botão estiver visível e marcado.
    """
    botao = page.locator(id_botao)

    if botao.is_visible():
        src = botao.get_attribute("src") or ""
        if "ckChecked" in src:
            botao.click()


def extrair_grupo_cota_sistema(texto: str):
    """
    Extrai grupo e cota de um texto da tabela de forma robusta.

    Exemplos aceitos:
    - 1628/980
    - 001628/0980
    - 1628 / 980
    - 1628/980-1
    - 001628/0980 - JOAO
    """
    texto = (texto or "").strip()

    match = re.search(r"(\d+)\s*/\s*(\d+)", texto)
    if not match:
        return None, None

    grupo = match.group(1).zfill(6)
    cota = match.group(2).zfill(4)
    return grupo, cota


# ============================================================
# LEITURA E ANÁLISE DA TABELA DE BOLETOS
# ============================================================

def analisar_tabela_boletos(page, data_vencimento_formatada, id_fila_adm):
    """
    Lê a tabela de boletos e monta um resumo com:
    - linhas válidas para emissão
    - quantidade de parcelas
    - quantidade de parcelas em atraso
    - meses encontrados

    Regras aplicadas:
    - ignora linhas sem colunas
    - ignora itens cujo histórico não contenha 'PARCELA'
    - ignora parcelas com dia >= 15
    - verifica se a cota existe na fila
    """
    tabela = page.locator("#ctl00_Conteudo_grdBoleto_Avulso")
    tabela.wait_for()
    linhas = tabela.locator("tr")

    resultado = []
    meses_parcelas = []

    meses_extenso = {
        "01": "Janeiro",
        "02": "Fevereiro",
        "03": "Março",
        "04": "Abril",
        "05": "Maio",
        "06": "Junho",
        "07": "Julho",
        "08": "Agosto",
        "09": "Setembro",
        "10": "Outubro",
        "11": "Novembro",
        "12": "Dezembro"
    }

    parcelas = 0
    parcelas_atraso = 0

    dia_ref, mes_ref, ano_ref = data_vencimento_formatada.split("/")

    for i in range(linhas.count()):
        colunas = linhas.nth(i).locator("td")

        if colunas.count() == 0:
            continue

        historico = colunas.nth(3).inner_text().strip()
        numero_html = i + 2
        id_botao = f"#ctl00_Conteudo_grdBoleto_Avulso_ctl{numero_html:02d}_imgEmite_Boleto"

        # Mantém selecionado apenas o que realmente for parcela
        if "PARCELA" not in historico.upper():
            continue

        vencimento = colunas.nth(4).inner_text().strip()
        dia_linha, mes_linha, ano_linha = vencimento.split("/")

        # Regra atual: ignorar parcelas com vencimento a partir do dia 15
        if int(dia_linha) >= 15:
            continue

        parcelas += 1

        # Conta atraso comparando o mês da linha com o mês de referência
        if mes_linha != mes_ref:
            parcelas_atraso += 1

        meses_parcelas.append(meses_extenso[mes_linha])

        texto_cota_sistema = colunas.nth(1).inner_text().strip()
        grupo_sistema, cota_formatada_sistema = extrair_grupo_cota_sistema(texto_cota_sistema)
    
        existe_na_fila = False
        if grupo_sistema and cota_formatada_sistema:
            existe_na_fila = verificar_cota_existe_na_fila(
                id_fila_adm,
                grupo_sistema,
                cota_formatada_sistema
            )
            
            

        resultado.append({
            "linha_index": i,
            "id_botao": id_botao,
            "existe_na_fila": existe_na_fila,
            "grupo_sistema": grupo_sistema,
            "cota_formatada_sistema": cota_formatada_sistema,
            "texto_cota_sistema": texto_cota_sistema
        })
   
    return {
        "resultado": resultado,
        "parcelas": parcelas,
        "parcelas_atraso": parcelas_atraso,
        "meses_parcelas": meses_parcelas
    }


# ============================================================
# PROCESSAMENTO DAS LINHAS VÁLIDAS
# ============================================================

def processar_resultado_boletos(
    page,
    resultado,
    id_fila_adm,
    nome_cliente,
    parcelas,
    clicou_unificar
):
    """
    Percorre apenas as linhas aprovadas na análise da tabela.

    Para cada linha:
    - marca a parcela para emissão
    - se a cota não existir na fila, registra no banco
    - se houver unificação, atualiza as cotas envolvidas
    """
    for linha in resultado:
        id_botao = linha["id_botao"]
        existe_na_fila = linha["existe_na_fila"]
        grupo_sistema = linha["grupo_sistema"]
        cota_formatada_sistema = linha["cota_formatada_sistema"]

        # Marca a parcela para emissão
        page.locator(id_botao).click()

        # Não conseguiu extrair grupo/cota da tela -> ignora update dessa linha
        if not grupo_sistema or not cota_formatada_sistema:
            continue

        # Se a cota não existir na fila, registra para tratamento posterior
        if not existe_na_fila:
            inserir_cotas_nao_encontradas(id_fila_adm,nome_cliente,grupo_sistema,cota_formatada_sistema)
            continue
       
        if parcelas > 1 and clicou_unificar:
            atualizar_status_unificados(
                id_fila_adm,
                grupo_sistema,
                cota_formatada_sistema,
                "UNIFICADO",
                "Boleto unificado!"
            )


        # Se houver mais de uma parcela e a unificação foi acionada,
        # atualiza as cotas participantes como unificadas
        


# ============================================================
# REGRAS DE NEGÓCIO - STATUS FINAL
# ============================================================

def decidir_status_final(
    parcelas,
    parcelas_atraso,
    meses_parcelas,
    clicou_unificar
):
    """
    Decide o status final da cota com base no resumo da tabela.
    A ordem das regras importa.
    """

    # Nenhuma parcela encontrada: considera adiantado
    if parcelas == 0:

        return {
            "status": "ADIANTADO",
            "observacao": "Todas as cotas foram pagas!"
        }

    # Havendo mais de uma parcela e unificação ativa, prevalece status unificado
    if parcelas > 1 and clicou_unificar:
        return {
            "status": "UNIFICADO",
            "observacao": "Boleto unificado!"
        }

    # Mais de 2 parcelas fora do mês de referência: atraso com atenção especial
    if parcelas_atraso > 2:
        return {
            "status": "EM ATRASO",
            "observacao": f"Verificar diluição! {parcelas_atraso} parcelas em atraso"
        }

    # Pelo menos 1 parcela em atraso
    if parcelas_atraso > 0:
        return {
            "status": "EM ATRASO",
            "observacao": f"{parcelas_atraso} parcelas em atraso"
        }

    # Caso normal sem atraso
    return {
        "status": "NORMAL",
        "observacao": f"Nenhuma parcela em atraso"
    }


# ============================================================
# MONTAGEM DO CAMINHO DO BOLETO
# ============================================================

def montar_caminho_boleto(
    caminho_lote,
    nome_consultor,
    nome_cliente,
    nome_cliente_sistema,
    meses_parcelas,
    parcelas,
    clicou_unificar
):
    """
    Monta o caminho final do boleto e garante que a pasta do consultor exista.

    Estrutura final:
    caminho_lote/
        Boletos/
            NOME_CONSULTOR/
                arquivo.pdf
    """

    nome_consultor = str(nome_consultor).strip()

    pasta_consultor = Path(caminho_lote) / "Boletos" / nome_consultor
    pasta_consultor.mkdir(parents=True, exist_ok=True)

    meses_parcelas_texto = " ".join(meses_parcelas).strip()

    if parcelas > 1 and clicou_unificar:
        nome_arquivo = f"Boleto Unificado {nome_cliente}"
    else:
        nome_arquivo = f"{meses_parcelas_texto} {nome_cliente_sistema}".strip()

    return str(pasta_consultor / nome_arquivo)


# ============================================================
# FINALIZAÇÃO DO PROCESSAMENTO
# ============================================================

def finalizar_processamento(
    id_cota,
    id_fila_adm,   # <<< adicionar
    caminho_lote,
    nome_consultor,
    nome_cliente,
    nome_cliente_sistema,
    meses_parcelas,
    parcelas,
    clicou_unificar,
    status_info
):
    """
    Aplica o status final no banco e, se houver boleto para emissão,
    registra também o caminho onde ele será salvo.
    """
    status = status_info["status"]
    observacao = status_info["observacao"]

    atualizar_status(id_cota, status, observacao)

    atualizar_contador_status(id_fila_adm, status)

    # Se estiver adiantado, não há boleto para gerar
    if status == "ADIANTADO":
        return None

    caminho_boleto = montar_caminho_boleto(
        caminho_lote,
        nome_consultor,
        nome_cliente,
        nome_cliente_sistema,
        meses_parcelas,
        parcelas,
        clicou_unificar
    )

    atualizar_caminho_boleto(id_cota, caminho_boleto)
    return caminho_boleto