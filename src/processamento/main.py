from playwright.sync_api import sync_playwright
import sys
import os
from dotenv import load_dotenv
from datetime import datetime
from time import sleep


SRC_DIR = os.path.dirname(os.path.dirname(__file__))
ROOT_DIR = os.path.dirname(SRC_DIR)

sys.path.insert(0, SRC_DIR)

ENV_PATH = os.path.join(ROOT_DIR, ".env")
load_dotenv(ENV_PATH, override=True)
from processamento.lib.db import get_conn
def atribuir_parametros():
    print("Python exe:", sys.executable, flush=True)
    print("Diretório atual:", os.getcwd(), flush=True)
    print("Argumentos recebidos:", sys.argv, flush=True)

    if len(sys.argv) < 2:
        print("ERRO: parâmetros não recebidos", flush=True)
        sys.exit(1)
    id_cota = sys.argv[1]
    print(f"id_cota recebido: {id_cota}", flush=True)

    with get_conn() as conexao:
        with conexao.cursor() as cur:
            cur.execute(
            """
            select nome_cliente,grupo,cota,nome_consultor,observacao
            from tbl_fila_cotas
            where id_cota = %s
            """,(id_cota,)
            )
            row = cur.fetchone()
        if not row:
            raise ValueError(f"id_Cota: {id_cota} não identificado no banco de dados")
        with conexao.cursor() as cur2:
            cur2.execute(
            """
            SELECT data_vencimento
            FROM tbl_fila_adm a
            JOIN tbl_fila_cotas c ON a.id_adm = c.id_adm
            WHERE a.id_fila_adm = (SELECT id_fila_adm FROM tbl_fila_cotas WHERE id_cota = %s)
            LIMIT 1 
            """,(id_cota,)
            )
            row_data = cur2.fetchone()
        if not row_data:
                raise ValueError(f"Data de vencimento não encontrada")
    return row, row_data

try:

    """row,row_data = atribuir_parametros()
    print("Retornou o atribuir parametros",row,row_data)

    nome_cliente = str(row[0])
    grupo = str(row[1])
    cota = str(row[2])
    nome_consultor = str(row[3])
    observacao = str(row[4])

    data_vencimento = str(row_data[0])"""
    data_vencimento = "2026-04-07"
    data_vencimento_formatada = datetime.strptime(
        str(data_vencimento),
        "%Y-%m-%d"
    ).strftime("%d/%m/%Y")

    URL_TRECHO = "https://newcon.ademicon.com.br/n4/www/frmMain.aspx"

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
            #print("Sem aba aberta",row,row_data)
            raise Exception("Não achei a aba do Newcon aberta")
        page = page_alvo

        page.bring_to_front()
        page.wait_for_load_state("networkidle")

        print("Conectado na aba:", page.url)

        # fluxo continua normalmente
        page.locator("#ctl00_img_Atendimento").click()
        page.locator("#ctl00_Conteudo_edtGrupo").fill("1628")
        page.locator("#ctl00_Conteudo_edtCota").fill("980")
        page.locator("#ctl00_Conteudo_btnLocalizar").click()
        page.locator("#ctl00_Conteudo_Menu_CONAT_grdMenu_CONAT_ctl10_hlkFormulario").click()
        nome_cliente = page.locator("#ctl00_Conteudo_Cabecalho_dados_cota_lblCD_Grupo").inner_text()
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

        for i in range(linhas.count()):
            colunas = linhas.nth(i).locator("td")
            if colunas.count() == 0:
                continue
                
            linha = {
                "linha_index":i,
                "cota_sistema":colunas.nth(1).inner_text().strip(),
                "historico": colunas.nth(3).inner_text().strip(),
                "vencimento": colunas.nth(4).inner_text().strip(),
            }
            resultado.append(linha)

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

        meses_atrasados = []
        linha_para_clicar = None

        for linha in resultado:
            historico = linha["historico"].upper()
            vencimento = linha["vencimento"]

            numero_html = linha["linha_index"] + 2
            id_botao = f"#ctl00_Conteudo_grdBoleto_Avulso_ctl{numero_html:02d}_imgEmite_Boleto"

            if "PARCELA" not in historico:
                if page.locator(id_botao).is_visible():
                    src = page.locator(id_botao).get_attribute("src") or ""
                    if "ckChecked" in src:
                        page.locator(id_botao).click()
                continue
            dia_linha,mes_linha,ano_linha = vencimento.split("/")
            dia_ref,mes_ref,ano_ref = data_vencimento_formatada.split("/")

            if int(dia_linha) < 15:
                if mes_linha != mes_ref:
                    status = "EM ATRASO"
                mes_nome = meses_extenso[mes_linha]
                meses_atrasados.append(mes_nome)
                linha_para_clicar = id_botao
                page.locator(linha_para_clicar).click()

                
            if not linha_para_clicar:
                raise Exception("Nenhuma linha PARCELA encontrada para a data informada")
            
            

        page.locator("#ctl00_Conteudo_btnEmitir").click()


        print("Fluxo executado com sucesso")
except Exception as e:
    print("ERRO NO PYTHON:", str(e), flush=True)
    sys.exit(1)