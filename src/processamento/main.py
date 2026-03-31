import sys
import os
from dotenv import load_dotenv

# adiciona /src no path
SRC_DIR = os.path.dirname(os.path.dirname(__file__))
ROOT_DIR = os.path.dirname(SRC_DIR)

sys.path.insert(0, SRC_DIR)

# carrega .env da raiz do projeto
ENV_PATH = os.path.join(ROOT_DIR, ".env")
load_dotenv(ENV_PATH, override=True)

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
from time import sleep
from pathlib import Path
import re

from shared.log import log_info, log_erro

from processamento.lib.funcoes_sql import (atribuir_parametros,
    atualizar_status_erro,
    atualizar_status,
    inserir_cotas_nao_encontradas,
    atualizar_caminho_boleto,
    verificar_cota_existe_na_fila,
    atualizar_status_unificados)

def clicar_seguro(page, selector, nome="botão", timeout=10000):
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

try:
    atribuir_parametros()
    id_cota,row,row_data = atribuir_parametros()
    print("Retornou o atribuir parametros",row,row_data)
    nome_cliente = str(row[0])
    grupo = str(row[1]).zfill(6)
    cota = str(row[2]).zfill(4)
    nome_consultor = str(row[3])
    pode_unificar = str(row[4])

    id_fila_adm = str(row_data[0])
    data_vencimento = str(row_data[1])
    caminho_lote = str(row_data[2])
    caminho_log = str(row_data[3])
    
    caminho_falha = fr"{caminho_lote}\Boletos\FALHAS"

    data_vencimento_formatada = datetime.strptime(
        str(data_vencimento),
        "%Y-%m-%d"
    ).strftime("%d/%m/%Y")
    

    URL_TRECHO = "newcon.ademicon.com.br"

    with sync_playwright() as p:

        # conecta no edge que já está aberto
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")

        context = browser.contexts[0]

        page_alvo = None

        for page in context.pages:
            if URL_TRECHO.lower() in page.url.lower():
                page_alvo = page
                break

        if not page_alvo:
            print("Sem aba aberta",row,row_data)
            raise Exception("Não achei a aba do Newcon aberta")
        page = page_alvo

        page.bring_to_front()
        page.wait_for_load_state("networkidle")

        print("Conectado na aba:", page.url)
        # fluxo continua normalmente
        page.locator("#ctl00_img_Atendimento").click()
        page.locator("#ctl00_Conteudo_edtGrupo").fill(grupo)
        page.locator("#ctl00_Conteudo_edtCota").fill(cota)
        page.locator("#ctl00_Conteudo_btnLocalizar").click()
        sleep(1)
        locator_erro = page.locator("#ctl00_Conteudo_lblErrMsg")
        if locator_erro.count() > 0:
            texto_erro = locator_erro.first.inner_text().strip()
            
            texto_limpo = re.sub(r'[\\/*?:"<>|]', "", texto_erro)[:60]

            if texto_erro:
                Path(caminho_falha).mkdir(parents=True, exist_ok=True)
                nome_arquivo = f"FALHA_{grupo}_{cota}_{texto_limpo}.png"
                caminho_print = Path(caminho_falha) / nome_arquivo
                caminho_print_str = str(caminho_print)

                page.screenshot(path=str(caminho_print_str), full_page=True)

                print("Mensagem encontrada:", texto_erro)
                print("Print salvo em:", caminho_print)
                atualizar_status_erro(id_cota,"FALHA",texto_limpo,caminho_print_str)
                sys.exit(0)

        page.locator("#ctl00_Conteudo_Menu_CONAT_grdMenu_CONAT_ctl10_hlkFormulario").click()
        clicou_unificar = False
        if pode_unificar.upper() == "SIM":
            botao_unificar = page.locator("#ctl00_Conteudo_chkUnificarParcelas")

            if botao_unificar.count() > 0:
                botao_unificar.click()
                clicou_unificar = True

        nome_cliente_sistema = page.locator("#ctl00_Conteudo_Cabecalho_dados_cota_lblCD_Grupo").inner_text()
        page.locator("#ctl00_Conteudo_edtDT_Compensacao").fill(data_vencimento_formatada)
        page.locator("#ctl00_Conteudo_edtDT_Base_Pendencias").fill(data_vencimento_formatada)
        page.locator("#ctl00_Conteudo_ckSelecionar_Todas").click()
        page.locator("#ctl00_Conteudo_ckSelecionar_Todas").click()
        page.locator("#ctl00_Conteudo_btnLocalizar").click()

        sleep(1)
        
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

        for i in range(linhas.count()):
            colunas = linhas.nth(i).locator("td")

            if colunas.count() == 0:
                continue

            historico = colunas.nth(3).inner_text().strip()
            numero_html = i + 2
            id_botao = f"#ctl00_Conteudo_grdBoleto_Avulso_ctl{numero_html:02d}_imgEmite_Boleto"
            if "PARCELA" not in historico.upper():
                if page.locator(id_botao).is_visible():
                    src = page.locator(id_botao).get_attribute("src") or ""
                    if "ckChecked" in src:
                        page.locator(id_botao).click()
                continue

            vencimento = colunas.nth(4).inner_text().strip()
            dia_linha,mes_linha,ano_linha = vencimento.split("/")
            dia_ref,mes_ref,ano_ref = data_vencimento_formatada.split("/")

            if int(dia_linha) >= 15:
                if page.locator(id_botao).is_visible():
                    src = page.locator(id_botao).get_attribute("src") or ""
                    if "ckChecked" in src:
                        page.locator(id_botao).click()
                continue
                
            parcelas += 1
            if mes_linha != mes_ref:
                parcelas_atraso += 1
            
            mes_nome = meses_extenso[mes_linha]
            meses_parcelas.append(mes_nome)
            
            cota_sistema = colunas.nth(1).inner_text().strip()
            grupo_sistema = cota_sistema.split("/")[0].zfill(6)
            cota_formatada_sistema = cota_sistema.split("/")[1].split("-")[0].zfill(4)

            existe_na_fila = verificar_cota_existe_na_fila(
                id_fila_adm,
                grupo_sistema,
                cota_formatada_sistema
            )

            linha = {
                "linha_index": i,
                "id_botao": id_botao,
                "existe_na_fila": existe_na_fila,
                "grupo_sistema": grupo_sistema,
                "cota_formatada_sistema": cota_formatada_sistema
            }

            resultado.append(linha)
            
            
        for linha in resultado:
            id_botao = linha["id_botao"]
            existe_na_fila = linha["existe_na_fila"]
            grupo_sistema = linha["grupo_sistema"]
            cota_formatada_sistema = linha["cota_formatada_sistema"]
            page.locator(id_botao).click()

            if not existe_na_fila:
                inserir_cotas_nao_encontradas(
                    id_fila_adm,
                    nome_cliente,
                    grupo_sistema,
                    cota_formatada_sistema
                )
                continue
                        
            if parcelas > 1 and clicou_unificar == True:
                status = "UNIFICADO"
                observacao = "Boleto unificado!"
                atualizar_status_unificados(
                        id_fila_adm,
                        grupo_sistema,
                        cota_formatada_sistema,
                        status,
                        observacao
                )
                
                
        #mas e se tiver só linha de imóvel?
        if parcelas == 0:
            status = "ADIANTADO"
            observacao = "Todas as cotas foram pagas!"
            atualizar_status(id_cota,status,observacao)
            sys.exit(0)

        if parcelas_atraso > 2:
            status = "EM ATRASO"
            observacao = f"Verificar diluição! {parcelas_atraso} parcelas em atraso"
        elif parcelas_atraso > 0:
            status = "EM ATRASO"
            observacao = f"{parcelas_atraso} parcelas em atraso"
        

        meses_parcelas_texto = ""
        for mes in meses_parcelas:
            meses_parcelas_texto += mes + " "

        if parcelas > 1 and clicou_unificar == True:
            status = "UNIFICADO"
            caminho_boleto = fr"{caminho_lote}\Boletos\{nome_consultor}\Boleto Unificado {nome_cliente}"
        else:
            caminho_boleto = fr"{caminho_lote}\Boletos\{nome_consultor}\{meses_parcelas_texto}{nome_cliente_sistema}"
        atualizar_caminho_boleto(id_cota,caminho_boleto)

        clicar_seguro(page, "#ctl00_Conteudo_btnEmitir", "botão Emitir")
        
        
        sys.exit(0)
except Exception as e:
    print("ERRO NO PYTHON:", str(e), flush=True)
    sys.exit(1)