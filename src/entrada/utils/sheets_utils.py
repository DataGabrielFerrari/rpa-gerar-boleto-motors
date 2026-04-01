import re

def extrair_id_planilha(link: str) -> str:
    if not link:
        raise ValueError("link_planilha está vazio.")
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", link)
    if not m:
        raise ValueError(f"Não consegui extrair o ID da planilha do link: {link}")
    return m.group(1)

def ler_range(service, spreadsheet_id: str, range_a1: str):
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        majorDimension="ROWS"
    ).execute()
    return resp.get("values", [])

def coluna_para_letra(idx_zero_based: int) -> str:
    idx = idx_zero_based + 1
    letras = ""
    while idx > 0:
        idx, resto = divmod(idx - 1, 26)
        letras = chr(65 + resto) + letras
    return letras

def atualizar_boleto_em_lote(service, spreadsheet_id: str, aba: str, letra_col_boleto: str, linhas: list[int]):
    if not linhas:
        return

    data = []
    for row_num in linhas:
        rng = f"{aba}!{letra_col_boleto}{row_num}"
        data.append({"range": rng, "values": [["NÃO BAIXADO"]]})

    body = {"valueInputOption": "RAW", "data": data}

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()