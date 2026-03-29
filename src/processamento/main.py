from playwright.sync_api import sync_playwright
import subprocess
import time
import sys

import os

def atribuir_parametros():
    print("Python exe:", sys.executable, flush=True)
    print("Diretório atual:", os.getcwd(), flush=True)
    print("Argumentos recebidos:", sys.argv, flush=True)

    if len(sys.argv) < 3:
        print("ERRO: parâmetros não recebidos", flush=True)
        sys.exit(1)
    nome_cliente = sys.argv[1]
    consultor = sys.argv[2]
    grupo = sys.argv[3]
    cota = sys.argv[4]

    print(f"Nome do cliente recebido: {nome_cliente}",flush=True)
    print(f"Consultor recebido {consultor}",flush=True)
    print(f"Grupo recebido: {grupo}", flush=True)
    print(f"Cota recebida: {cota}", flush=True)


    return nome_cliente,consultor,grupo,cota

atribuir_parametros()

EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
URL = "https://newcon.ademicon.com.br/n4/www/frmMain.aspx"

# fecha qualquer edge aberto
subprocess.run("taskkill /F /IM msedge.exe", shell=True)

time.sleep(2)

# abre edge com debug ativo
subprocess.Popen([
    EDGE_PATH,
    "--remote-debugging-port=9222",
    "--disable-popup-blocking",
    URL
])

time.sleep(5)

URL_TRECHO = "newcon.ademicon.com.br/n4/www/"

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
    context = browser.contexts[0]

    page_alvo = None
    for page in context.pages:
        if URL_TRECHO.lower() in page.url.lower():
            page_alvo = page
            break

    if not page_alvo:
        raise Exception("Não achei a aba do Newcon aberta")

    page = page_alvo
    page.bring_to_front()
    page.wait_for_load_state()

    print("Conectado na aba:", page.url)

    page.locator("#ctl00_img_Atendimento").click()
    page.locator("#ctl00_Conteudo_edtGrupo").fill("1645")
    page.locator("#ctl00_Conteudo_edtCota").fill("623")
    page.locator("#ctl00_Conteudo_btnLocalizar").click()
    page.locator("#ctl00_Conteudo_Menu_CONAT_grdMenu_CONAT_ctl10_hlkFormulario").click()
    page.locator("#ctl00_Conteudo_grdBoleto_Avulso_ctl03_imgEmite_Boleto").click()
    page.locator("#ctl00_Conteudo_btnEmitir").click()
    sys.exit(0)

    



