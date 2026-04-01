BLOQUEADOS = {
    "DDA",
    "CC",
    "CANCELADO",
    "NAO PROCESSAR",
    "NÃO PROCESSAR",
}

def status_boleto(texto: str) -> str:
    return (texto or "").strip().upper()

def deve_bloquear(status: str) -> bool:
    return status in BLOQUEADOS

def esta_nao_baixado(status: str) -> bool:
    return status in ("NÃO BAIXADO", "NAO BAIXADO")