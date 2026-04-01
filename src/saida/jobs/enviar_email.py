import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from saida.lib.db import get_conn
from shared.log import log_info, log_erro

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)

TOKEN_PATH = os.path.join(PROJECT_ROOT, "credentials", "token.json")
CLIENT_SECRET_PATH = os.path.join(PROJECT_ROOT, "credentials", "client_secret.json")


def formatar_mes_extenso(mes_ref: int) -> str:
    mes_ref_str = str(mes_ref)

    ano = mes_ref_str[:4]
    mes = int(mes_ref_str[4:6])

    meses = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }

    mes_nome = meses.get(mes, "Mês inválido")
    return f"{mes_nome}/{ano}"


def _get_gmail_service(id_fila_adm):

    if not os.path.exists(TOKEN_PATH):

        log_erro(
            "EMAIL",
            id_fila_adm,
            "Validar token",
            f"token.json não encontrado: {TOKEN_PATH}"
        )

        raise FileNotFoundError(TOKEN_PATH)

    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if creds and creds.expired and creds.refresh_token:

        log_info(
            "EMAIL",
            id_fila_adm,
            "Refresh token",
            "Token expirado. Tentando refresh automático"
        )

        try:
            creds.refresh(Request())

            with open(TOKEN_PATH, "w", encoding="utf-8") as f:
                f.write(creds.to_json())

        except Exception as e:

            log_erro(
                "EMAIL",
                id_fila_adm,
                "Refresh token",
                str(e)
            )

            raise

    if not creds or not creds.valid:

        log_erro(
            "EMAIL",
            id_fila_adm,
            "Validar credenciais",
            "Credenciais inválidas. Refazer token com access_type=offline"
        )

        raise RuntimeError(
            "Credenciais Gmail inválidas (sem refresh_token ou escopos divergentes)"
        )

    return build("gmail", "v1", credentials=creds)


def _buscar_clientes_nao_encontrados(id_fila_adm: int):

    conn = get_conn()

    try:

        with conn.cursor() as cur:

            cur.execute("""
                SELECT nome_cliente, grupo, cota
                FROM tbl_cotas_nao_encontradas
                WHERE id_fila_adm = %s
                ORDER BY grupo, cota
            """, (id_fila_adm,))

            rows = cur.fetchall()

        log_info(
            "EMAIL",
            id_fila_adm,
            "Nao encontrados",
            f"qtd={len(rows)}"
        )

        return rows

    finally:
        conn.close()


def enviar_email_lote(id_fila_adm: int):

    conn = get_conn()

    try:

        with conn.cursor() as cur:

            cur.execute("""
                SELECT f.mes_ref,
                       f.clientes_sucesso,
                       f.clientes_erro,
                       f.clientes_com_atraso,
                       f.clientes_adiantados,
                       f.link_drive,
                       a.email,
                       a.nome
                FROM tbl_fila_adm f
                JOIN tbl_adm a ON a.id_adm = f.id_adm
                WHERE f.id_fila_adm = %s
            """, (id_fila_adm,))

            row = cur.fetchone()

    finally:
        conn.close()

    if not row:

        log_erro(
            "EMAIL",
            id_fila_adm,
            "Buscar dados lote",
            "Lote não encontrado"
        )

        return

    (
        mes_ref,
        clientes_sucesso,
        clientes_erro,
        clientes_com_atraso,
        clientes_adiantados,
        link_drive,
        email_destino,
        nome_adm,
    ) = row

    if not link_drive:

        log_erro(
            "EMAIL",
            id_fila_adm,
            "Validar link",
            "link_drive vazio"
        )

        return

    if not email_destino:

        log_erro(
            "EMAIL",
            id_fila_adm,
            "Validar email",
            f"ADM sem email cadastrado: {nome_adm}"
        )

        return

    mes_ref = formatar_mes_extenso(mes_ref)

    assunto = f"Boletos Motors - Resumo Processamento | {mes_ref}"

    nao_encontrados = _buscar_clientes_nao_encontrados(id_fila_adm)

    if nao_encontrados:

        linhas_txt = []

        for nome_cliente, grupo, cota in nao_encontrados:

            nome_cliente = (nome_cliente or "").strip() or "(sem nome)"

            linhas_txt.append(
                f"- {nome_cliente} | Grupo: {grupo} | Cota: {cota}"
            )

        secao_nao_encontrados_txt = (
            "\nCotas registradas no sistema e não localizadas na planilha:\n"
            + "\n".join(linhas_txt)
            + "\n"
        )

    else:

        secao_nao_encontrados_txt = (
            "\nCotas registradas no sistema e não localizadas na planilha: 0\n"
        )

    if nao_encontrados:

        itens_li = []

        for nome_cliente, grupo, cota in nao_encontrados:

            nome_cliente = (nome_cliente or "").strip() or "(sem nome)"

            itens_li.append(
                f"<li><strong>{nome_cliente}</strong> — Grupo: {grupo} | Cota: {cota}</li>"
            )

        secao_nao_encontrados_html = f"""
          <h3 style="margin:16px 0 8px; color:#444;">
            ⚠ Cotas registradas no sistema e não localizadas na planilha:
          </h3>

          <ul style="margin:0; padding-left:18px;">
            {''.join(itens_li)}
          </ul>
        """

    else:

        secao_nao_encontrados_html = """
          <h3 style="margin:16px 0 8px; color:#444;">
            ✅ Cotas registradas no sistema e não localizadas na planilha
          </h3>

          <p style="margin:0;">0</p>
        """

    corpo_txt = f"""
Resumo de Processamento – Boletos Motors

Administrador: {nome_adm}
Mês de vencimento: {mes_ref}

Resultado:

- Clientes Normais: {clientes_sucesso}
- Clientes com Erro: {clientes_erro}
- Clientes com Atraso: {clientes_com_atraso}
- Clientes Adiantados: {clientes_adiantados}

{secao_nao_encontrados_txt}

Link Drive:
{link_drive}

Este e-mail foi gerado automaticamente pelo sistema RPA.
""".strip()

    corpo_html = f"""<html>
<body style="font-family: Arial, sans-serif; font-size:14px; color:#333;">

<h2>Resumo de Processamento – Boletos Motors</h2>

<p>
<strong>Administrador:</strong> {nome_adm}<br>
<strong>Mês de vencimento:</strong> {mes_ref}
</p>

<h3>Resultado</h3>

<ul>
<li>Normais: {clientes_sucesso}</li>
<li>Erro: {clientes_erro}</li>
<li>Atraso: {clientes_com_atraso}</li>
<li>Adiantado: {clientes_adiantados}</li>
</ul>

{secao_nao_encontrados_html}

<p>
<a href="{link_drive}">Abrir no Drive</a>
</p>

</body>
</html>
"""

    service = _get_gmail_service(id_fila_adm)

    msg = MIMEMultipart("alternative")

    msg["to"] = email_destino
    msg["subject"] = assunto

    msg.attach(MIMEText(corpo_txt, "plain", "utf-8"))
    msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(
        msg.as_bytes()
    ).decode()

    service.users().messages().send(
        userId="me",
        body={"raw": raw}
    ).execute()

    log_info(
        "EMAIL",
        id_fila_adm,
        "Enviar email",
        f"destino={email_destino} nao_encontrados={len(nao_encontrados)}"
    )
