import os
import zipfile

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request

from saida.lib.db import get_conn
from shared.log import log_info, log_erro

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
    mes = mes_ref % 100
    if mes not in MESES_PT:
        raise ValueError(f"mes_ref inválido: {mes_ref}")
    return MESES_PT[mes]

def zipar_boletos(caminho_lote: str, nome_adm: str, mes_ref: int) -> str:
    boletos_dir = os.path.join(caminho_lote, "Boletos")

    print("ZIPANDO:", boletos_dir)

    arquivos_encontrados = 0

    nome_zip = f"{nome_adm}_{mes_extenso(mes_ref)}.zip"
    zip_path = os.path.join(caminho_lote, nome_zip)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(boletos_dir):
            for file in files:
                arquivos_encontrados += 1
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, boletos_dir)
                z.write(full_path, relative_path)

    print("Arquivos zipados:", arquivos_encontrados)

    return zip_path


def criar_link_drive(
    zip_path: str,
    nome_zip: str,
    caminho_log: str,
    id_fila_adm: int
) -> str:
    project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)

    token_path = os.path.join(project_root, "credentials", "token.json")

    if not os.path.exists(token_path):
        raise FileNotFoundError(f"token.json não encontrado em: {token_path}")

    creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        log_info(
            caminho_log=caminho_log,
            etapa="DRIVE",
            id_dado=id_fila_adm,
            acao="Refresh token",
            detalhe="Token expirado, tentando refresh"
        )

        creds.refresh(Request())

        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

        log_info(
            caminho_log=caminho_log,
            etapa="DRIVE",
            id_dado=id_fila_adm,
            acao="Refresh token",
            detalhe="Token atualizado com sucesso"
        )

    if not creds or not creds.valid:
        raise RuntimeError("Credenciais do Google Drive inválidas")

    service = build("drive", "v3", credentials=creds)

    media = MediaFileUpload(zip_path, resumable=True)

    file = service.files().create(
        body={"name": nome_zip},
        media_body=media,
        fields="id, webViewLink"
    ).execute()

    service.permissions().create(
        fileId=file["id"],
        body={"type": "anyone", "role": "reader"}
    ).execute()

    return file["webViewLink"]


def processar_drive_finalizados(id_fila_adm: int) -> int:
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    f.id_fila_adm,
                    f.mes_ref,
                    f.caminho_lote,
                    f.caminho_log,
                    a.nome
                FROM tbl_fila_adm f
                JOIN tbl_adm a ON a.id_adm = f.id_adm
                WHERE f.id_fila_adm = %s
                  AND TRIM(UPPER(f.status)) = 'FINALIZADO'
                  AND f.link_drive IS NULL
                  AND f.caminho_lote IS NOT NULL
                """,
                (id_fila_adm,)
            )
            row = cur.fetchone()

        if not row:
            return 0

        id_fila_adm_db, mes_ref, caminho_lote, caminho_log, nome_adm = row

        log_info(
            caminho_log=caminho_log,
            etapa="DRIVE",
            id_dado=id_fila_adm_db,
            acao="Processar lote",
            detalhe=f"Gerando zip para ADM={nome_adm}"
        )

        zip_path = zipar_boletos(caminho_lote, nome_adm, mes_ref)
        nome_zip = os.path.basename(zip_path)

        log_info(
            caminho_log=caminho_log,
            etapa="DRIVE",
            id_dado=id_fila_adm_db,
            acao="Upload Drive",
            detalhe=f"Arquivo={nome_zip}"
        )

        link = criar_link_drive(
            zip_path=zip_path,
            nome_zip=nome_zip,
            caminho_log=caminho_log,
            id_fila_adm=id_fila_adm_db
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tbl_fila_adm
                SET link_drive = %s
                WHERE id_fila_adm = %s
                """,
                (link, id_fila_adm_db),
            )

        conn.commit()

        log_info(
            caminho_log=caminho_log,
            etapa="DRIVE",
            id_dado=id_fila_adm_db,
            acao="Finalizar upload",
            detalhe=f"link_drive gerado com sucesso"
        )

        return 1

    except Exception as e:
        conn.rollback()

        caminho_log_erro = None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT caminho_log
                    FROM tbl_fila_adm
                    WHERE id_fila_adm = %s
                    """,
                    (id_fila_adm,)
                )
                row_log = cur.fetchone()
                if row_log:
                    caminho_log_erro = row_log[0]
        except Exception:
            pass

        if caminho_log_erro:
            log_erro(
                caminho_log=caminho_log_erro,
                etapa="DRIVE",
                id_dado=id_fila_adm,
                acao="Processar lote",
                detalhe=f"erro={e}"
            )

        raise

    finally:
        conn.close()