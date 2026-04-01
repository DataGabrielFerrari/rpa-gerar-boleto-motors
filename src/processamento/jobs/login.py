from playwright.sync_api import sync_playwright
import subprocess
import time
import sys
import os
import traceback
from dotenv import load_dotenv
import requests

# src
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
# raiz do projeto
ROOT_DIR = os.path.dirname(SRC_DIR)

# garante que "processamento" seja encontrado
sys.path.insert(0, SRC_DIR)

ENV_PATH = os.path.join(ROOT_DIR, ".env")
load_dotenv(ENV_PATH, override=True)

print("SRC_DIR:", SRC_DIR, flush=True)
print("ENV carregado de:", ENV_PATH, flush=True)
print("DB_HOST no login.py:", os.getenv("DB_HOST"), flush=True)

from processamento.lib.db import get_conn
import processamento.lib.db as db_mod

print("db carregado de:", db_mod.__file__, flush=True)


def matar_edge_debug():
    """
    Fecha processos do Edge que possam estar atrapalhando a porta 9222.
    """
    try:
        subprocess.run(
            ["taskkill", "/F", "/IM", "msedge.exe"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False
        )
        time.sleep(2)
    except Exception:
        pass


def esperar_cdp(porta=9222, timeout=20):
    """
    Espera o endpoint de depuração do Edge responder.
    """
    inicio = time.time()

    while time.time() - inicio < timeout:
        try:
            r = requests.get(f"http://127.0.0.1:{porta}/json/version", timeout=1)
            if r.status_code == 200:
                print(f"CDP disponível na porta {porta}", flush=True)
                return True
        except Exception:
            pass

        time.sleep(1)

    return False


try:
    print("Python exe:", sys.executable, flush=True)
    print("Diretorio atual:", os.getcwd(), flush=True)
    print("Argumentos recebidos:", sys.argv, flush=True)

    if len(sys.argv) < 2:
        print("ERRO: parametro nao recebido", flush=True)
        sys.exit(1)

    id_fila_adm = sys.argv[1]
    print("Vai consultar o banco", flush=True)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select a.matricula, a.senha
                from tbl_adm a
                join tbl_fila_adm f on a.id_adm = f.id_adm
                where f.id_fila_adm = %s
                """,
                (id_fila_adm,)
            )
            row = cur.fetchone()

    if not row:
        raise ValueError("Retornou matricula e senha vazio")

    matricula = str(row[0]).strip()
    senha = str(row[1]).strip()

    EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
    if not os.path.exists(EDGE_PATH):
        EDGE_PATH = r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"

    if not os.path.exists(EDGE_PATH):
        raise FileNotFoundError("Nao encontrei o msedge.exe")

    URL = "https://newcon.ademicon.com.br/n4/www/"
    PORTA = 9222

    print("Fechando Edge antigo...", flush=True)
    matar_edge_debug()

    print("Abrindo Edge com CDP...", flush=True)

    subprocess.Popen([
        EDGE_PATH,
        f"--remote-debugging-port={PORTA}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-popup-blocking",
        "--start-maximized",
        URL
    ])

    if not esperar_cdp(PORTA, 20):
        raise Exception(f"Edge nao abriu com depuracao remota na porta {PORTA}")

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(f"http://127.0.0.1:{PORTA}")

        if not browser.contexts:
            raise Exception("Nenhum contexto encontrado no Edge")

        context = browser.contexts[0]
        page_alvo = None

        for page in context.pages:
            try:
                url_atual = page.url.lower()
                print("Aba encontrada:", url_atual, flush=True)

                if "frmcorcccnslogin.aspx" in url_atual:
                    page_alvo = page
                    break
            except Exception:
                pass

        if not page_alvo:
            for page in context.pages:
                try:
                    url_atual = page.url.lower()
                    if "newcon.ademicon.com.br/n4/www/" in url_atual:
                        page_alvo = page
                        break
                except Exception:
                    pass

        if not page_alvo:
            if context.pages:
                page_alvo = context.pages[0]
            else:
                page_alvo = context.new_page()
                page_alvo.goto(URL, wait_until="load")

        page = page_alvo
        page.bring_to_front()
        page.wait_for_load_state("load")

        print("Conectado na aba:", page.url, flush=True)

        page.locator("#edtUsuario").wait_for(timeout=15000)
        page.locator("#edtUsuario").fill(matricula)
        page.locator("#edtSenha").fill(senha)
        page.locator("#btnLogin").click()

        print("LOGIN OK", flush=True)

except Exception as e:
    print("ERRO NO PYTHON:", str(e), flush=True)
    traceback.print_exc()
    sys.exit(1)