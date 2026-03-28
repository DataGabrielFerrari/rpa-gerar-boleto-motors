import os
import zipfile
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from saida.lib.db import get_conn

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]

MESES_PT = {
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


def mes_extenso(mes_ref: int) -> str:
    return MESES_PT[mes_ref % 100]


def zipar_boletos(caminho_lote: str, nome_adm: str, mes_ref: int) -> str:
    boletos_dir = os.path.join(caminho_lote, "Boletos")

    if not os.path.isdir(boletos_dir):
        raise Exception(f"Pasta Boletos não encontrada: {boletos_dir}")

    nome_zip = f"{nome_adm}_{mes_extenso(mes_ref)}.zip"
    zip_path = os.path.join(caminho_lote, nome_zip)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(boletos_dir):
            for file in files:
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, boletos_dir)
                z.write(full_path, relative_path)

    return zip_path


def criar_link_drive(zip_path: str, nome_zip: str, logger=None) -> str:
    import requests
    from google.auth.transport.requests import Request
    from googleapiclient.errors import HttpError

    base_dir = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(base_dir, "credentials", "token.json")

    if not os.path.exists(token_path):
        raise FileNotFoundError(f"token.json não encontrado em: {token_path}")

    creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    def log(msg: str):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    log(f"[DRIVE] token_path={token_path}")
    log(f"[DRIVE] creds.scopes={getattr(creds, 'scopes', None)}")
    log(f"[DRIVE] has_drive_scope={creds.has_scopes(['https://www.googleapis.com/auth/drive'])}")
    log(f"[DRIVE] valid={creds.valid} expired={creds.expired} has_refresh={bool(creds.refresh_token)}")

    # 1) GARANTE access_token ATUAL
    # Se não tiver token (None) ou estiver expirado, dá refresh.
    if (not getattr(creds, "token", None)) or (not creds.valid):
        if creds.refresh_token:
            log("[DRIVE] Refreshing access token...")
            creds.refresh(Request())
            log(f"[DRIVE] After refresh: valid={creds.valid} expired={creds.expired} token_present={bool(creds.token)}")
        else:
            # Sem refresh_token -> token inválido para operar em background
            raise RuntimeError("Token sem refresh_token. Refaça o OAuth com access_type='offline' e prompt='consent'.")

    # 2) TOKENINFO (DIAGNÓSTICO REAL)
    # Esse é o que vale: scopes efetivos do access token ATUAL.
    try:
        r = requests.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"access_token": creds.token},
            timeout=20
        )
        # tokeninfo retorna 200 com scope, ou 400 com error
        log(f"[TOKENINFO] status={r.status_code} body={r.text}")
    except Exception as e:
        log(f"[TOKENINFO] erro ao consultar tokeninfo: {e}")

    # 3) TRAVA se não tiver escopo de drive (pelo "contrato" do Credentials)
    if not creds.has_scopes(["https://www.googleapis.com/auth/drive"]):
        raise RuntimeError("Token OAuth sem escopo do Drive (creds.has_scopes=False). Refaça o OAuth pedindo scope drive.")

    # 4) EXECUTA DRIVE (com HttpError detalhado)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    media = MediaFileUpload(zip_path, resumable=True)
    try:
        file = service.files().create(
            body={"name": nome_zip},
            media_body=media,
            fields="id, webViewLink"
        ).execute()

        # libera "anyone with link"
        service.permissions().create(
            fileId=file["id"],
            body={"type": "anyone", "role": "reader"}
        ).execute()

        return file["webViewLink"]

    except HttpError as e:
        # isso te mostra o endpoint real que falhou e o JSON de erro
        content = getattr(e, "content", b"")
        try:
            content_txt = content.decode("utf-8", errors="replace")
        except Exception:
            content_txt = repr(content)

        log(f"[DRIVE HTTPERROR] status={getattr(e.resp, 'status', None)} uri={getattr(e, 'uri', None)} content={content_txt}")
        raise
    


def processar_drive_finalizados(logger):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT f.id_fila_adm, f.mes_ref, f.caminho_lote, a.nome
        FROM tbl_fila_adm f
        JOIN tbl_adm a ON a.id_adm = f.id_adm
        WHERE TRIM(UPPER(f.status)) = 'FINALIZADO'
          AND f.link_drive IS NULL
          AND f.caminho_lote IS NOT NULL
        ORDER BY f.id_fila_adm
        """
    )

    rows = cur.fetchall()

    if not rows:
        cur.close()
        conn.close()
        return 0

    enviados = 0

    for id_fila_adm, mes_ref, caminho_lote, nome_adm in rows:
        try:
            logger.info(f"[DRIVE] Processando lote {id_fila_adm}")

            zip_path = zipar_boletos(caminho_lote, nome_adm, mes_ref)
            nome_zip = os.path.basename(zip_path)

            link = criar_link_drive(zip_path, nome_zip,logger=logger)
            cur.execute(
                """
                UPDATE tbl_fila_adm
                SET link_drive = %s
                WHERE id_fila_adm = %s
                """,
                (link, id_fila_adm),
            )
            conn.commit()

            logger.info(f"[DRIVE OK] id_fila_adm={id_fila_adm} link={link}")
            enviados += 1

        except Exception as e:
            conn.rollback()
            logger.error(f"[DRIVE ERRO] id_fila_adm={id_fila_adm} erro={e}")

    cur.close()
    conn.close()
    return enviados