from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/drive"
]

TOKEN_PATH = "token.json"
CREDENTIALS_PATH = "credentials/client_secret.json"


def get_credentials():
    creds = None

    # 🔹 Carrega token existente
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # 🔹 Se inválido ou inexistente
    if not creds or not creds.valid:

        # Tenta refresh
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Erro ao dar refresh no token: {e}")
                creds = None

        # 🔥 Se não conseguiu, força novo login
        if not creds:
            if os.path.exists(TOKEN_PATH):
                os.remove(TOKEN_PATH)  # remove token quebrado

            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES
            )

            creds = flow.run_local_server(
                port=0,
                prompt='consent',  # 🔥 força tela de permissão
                access_type='offline'  # 🔥 garante refresh_token
            )

        # 🔹 Salva token novo
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    return creds

if __name__ == "__main__":
    creds = get_credentials()
    print("Credenciais geradas com sucesso")