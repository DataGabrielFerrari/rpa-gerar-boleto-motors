import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.auth.transport.requests import Request

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from saida.lib.db import get_conn

SCOPES = [
  "https://www.googleapis.com/auth/gmail.send",
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive",
]

# Ajuste para o seu projeto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",".."))
TOKEN_PATH = os.path.join(PROJECT_ROOT,"credentials","token.json")
CLIENT_SECRET_PATH = os.path.join(PROJECT_ROOT,"credentials","client_secret.json")

def formatar_mes_extenso(mes_ref: int) -> str:
    """
    Converte 202603 -> Março/2026
    """
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

def _get_gmail_service(logger):
    if not os.path.exists(TOKEN_PATH):
        logger.error(f"[GMAIL] token.json não encontrado: {TOKEN_PATH}")
        raise FileNotFoundError(TOKEN_PATH)

    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # 1) Se expirou mas tem refresh_token, renova sozinho
    if creds and creds.expired and creds.refresh_token:
        try:
            logger.info("[GMAIL] Token expirado. Tentando refresh automático...")
            creds.refresh(Request())
            # salva token atualizado (novo access_token / expiry)
            with open(TOKEN_PATH, "w", encoding="utf-8") as f:
                f.write(creds.to_json())
            logger.info("[GMAIL] Refresh OK. token.json atualizado.")
        except Exception as e:
            logger.error(f"[GMAIL] Falha no refresh: {e}")
            raise

    # 2) Se ainda não está válido aqui, aí sim você precisa refazer token (geralmente 1x)
    if not creds or not creds.valid:
        # Isso normalmente significa: sem refresh_token OU escopos inconsistentes
        logger.error(
            "[GMAIL] Credenciais inválidas. Provável token sem refresh_token "
            "ou token gerado com escopos diferentes. Refaça o token UMA vez com access_type=offline."
        )
        raise RuntimeError("Credenciais Gmail inválidas (sem refresh_token ou escopos divergentes)")

    return build("gmail", "v1", credentials=creds)

def _buscar_clientes_nao_encontrados(id_fila_adm: int, logger):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT nome_cliente, grupo, cota
        FROM tbl_clientes_nao_encontrados
        WHERE id_fila_adm = %s
        ORDER BY grupo, cota
    """, (id_fila_adm,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    logger.info(f"[NAO_ENCONTRADOS] qtd={len(rows)} id_fila_adm={id_fila_adm}")
    return rows


def enviar_email_lote(id_fila_adm: int, logger):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT f.mes_ref,
               f.clientes_processados,
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
    cur.close()
    conn.close()

    if not row:
        logger.error("[EMAIL] Lote não encontrado.")
        return

    (
        mes_ref,
        clientes_processados,
        clientes_sucesso,
        clientes_erro,
        clientes_com_atraso,
        clientes_adiantados,
        link_drive,
        email_destino,
        nome_adm,
    ) = row

    if not link_drive:
        logger.warning("[EMAIL] Sem link_drive.")
        return
    mes_ref = formatar_mes_extenso(mes_ref)
    # Assunto (aqui você padroniza)
    assunto = f"Boletos Motors - Resumo Processamento | {mes_ref}"

    # 1) Busca os clientes que estavam na planilha mas NÃO apareceram no sistema
    nao_encontrados = _buscar_clientes_nao_encontrados(id_fila_adm, logger)

    # -----------------------------
    # Seção NÃO encontrados (texto)
    # -----------------------------
    if nao_encontrados:
        linhas_txt = []
        for nome_cliente, grupo, cota in nao_encontrados:
            nome_cliente = (nome_cliente or "").strip() or "(sem nome)"
            linhas_txt.append(f"- {nome_cliente} | Grupo: {grupo} | Cota: {cota}")

        secao_nao_encontrados_txt = (
            "\nCotas do sistema que NÃO foram encontrados na planilha:\n"
            + "\n".join(linhas_txt)
            + "\n"
        )
    else:
        secao_nao_encontrados_txt = "\nCotas do sistema que NÃO foram encontrados na planilha: 0\n"

    # -----------------------------
    # Seção NÃO encontrados (HTML)
    # -----------------------------
    if nao_encontrados:
        itens_li = []
        for nome_cliente, grupo, cota in nao_encontrados:
            nome_cliente = (nome_cliente or "").strip() or "(sem nome)"
            itens_li.append(f"<li><strong>{nome_cliente}</strong> — Grupo: {grupo} | Cota: {cota}</li>")

        secao_nao_encontrados_html = f"""
          <h3 style="margin:16px 0 8px; color:#444;">⚠ Cotas registradas no sistema e não localizadas na planilha: </h3>
          <ul style="margin:0; padding-left:18px;">
            {''.join(itens_li)}
          </ul>
        """
    else:
        secao_nao_encontrados_html = """
          <h3 style="margin:16px 0 8px; color:#444;">✅ Clientes não encontrados no sistema</h3>
          <p style="margin:0;">0</p>
        """

    # -----------------------------
    # Corpo TEXTO (fallback)
    # -----------------------------
    corpo_txt = f"""
Resumo de Processamento – Boletos Motors

Administrador: {nome_adm}
Mês de vencimento: {mes_ref}

Resultado:
- Clientes Processados: {clientes_processados}
- Clientes com Sucesso: {clientes_sucesso}
- Clientes com Erro: {clientes_erro}
- Clientes com Atraso: {clientes_com_atraso}
- Clientes Adiantados: {clientes_adiantados}
{secao_nao_encontrados_txt}
Link Drive:
{link_drive}

Este e-mail foi gerado automaticamente pelo sistema RPA.
""".strip()

    # -----------------------------
    # Corpo HTML (bonito)
    # -----------------------------
    corpo_html = f"""\
<html>
  <body style="font-family: Arial, sans-serif; font-size:14px; color:#333; line-height:1.4;">
    <div style="max-width:720px; margin:0 auto;">
      <h2 style="color:#1a73e8; margin:0 0 10px;">Resumo de Processamento – Boletos Motors</h2>

      <p style="margin:0 0 12px;">
        <strong>Administrador:</strong> {nome_adm}<br>
        <strong>Mês de vencimento:</strong> {mes_ref}
      </p>

      <hr style="border:none; border-top:1px solid #e5e7eb; margin:12px 0;">

      <h3 style="margin:0 0 8px; color:#444;">📊 Resultado</h3>

      <table style="border-collapse:collapse; width:100%; max-width:520px;">
        <tr>
          <td style="padding:6px 10px; border:1px solid #e5e7eb;"><strong>Clientes Processados</strong></td>
          <td style="padding:6px 10px; border:1px solid #e5e7eb;">{clientes_processados}</td>
        </tr>
        <tr>
          <td style="padding:6px 10px; border:1px solid #e5e7eb;"><strong>Sucesso</strong></td>
          <td style="padding:6px 10px; border:1px solid #e5e7eb; color:#0f9d58;"><strong>{clientes_sucesso}</strong></td>
        </tr>
        <tr>
          <td style="padding:6px 10px; border:1px solid #e5e7eb;"><strong>Erro</strong></td>
          <td style="padding:6px 10px; border:1px solid #e5e7eb; color:#d93025;"><strong>{clientes_erro}</strong></td>
        </tr>
        <tr>
          <td style="padding:6px 10px; border:1px solid #e5e7eb;"><strong>Atraso</strong></td>
          <td style="padding:6px 10px; border:1px solid #e5e7eb; color:#d97706;"><strong>{clientes_com_atraso}</strong></td>
        </tr>
        <tr>
            <td style="padding:6px 10px; border:1px solid #e5e7eb;"><strong>Adiantado</strong></td>
            <td style="padding:6px 10px; border:1px solid #e5e7eb; color:#1a73e8;"><strong>{clientes_adiantados}</strong></td>
        </tr>
        </table>

      {secao_nao_encontrados_html}

      <hr style="border:none; border-top:1px solid #e5e7eb; margin:16px 0;">

      <h3 style="margin:0 0 8px; color:#444;">📁 Link Drive</h3>
      <p style="margin:0 0 12px;">
        <a href="{link_drive}" style="color:#1a73e8; text-decoration:none;">
          Abrir no Google Drive
        </a><br>
        <span style="font-size:12px; color:#666;">Se não abrir, copie e cole o link no navegador.</span>
      </p>

      <p style="font-size:12px; color:#777; margin:18px 0 0;">
        Este e-mail foi gerado automaticamente pelo sistema RPA.
      </p>
    </div>
  </body>
</html>
""".strip()

    # -----------------------------
    # Envio Gmail API (multipart)
    # -----------------------------
    service = _get_gmail_service(logger)

    msg = MIMEMultipart("alternative")
    msg["to"] = email_destino
    msg["subject"] = assunto

    msg.attach(MIMEText(corpo_txt, "plain", "utf-8"))
    msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    service.users().messages().send(
        userId="me",
        body={"raw": raw}
    ).execute()

    logger.info(f"[EMAIL OK] Enviado para {email_destino} | nao_encontrados={len(nao_encontrados)}")