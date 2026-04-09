"""
ETL - Leitura do Google Sheets e importacao para ancore.pendencias
Colunas A:H -> situacao, qtd_retorno, data, associado, placa_chassi, data_cadastro, cooperativa, consultor
"""

from google.oauth2 import service_account
from googleapiclient.discovery import build
import psycopg2
import dados
import os

# ==============================
# CONFIGURACOES GOOGLE SHEETS
# ==============================

SERVICE_ACCOUNT_FILE = 'credenciais.json'
SCOPES               = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID       = '1Dk_IWnwkWeeERNY6GYUxdWPD6cPkWEKLn0JaTQqQW2I'
RANGE_NAME           = 'Página1!A:H'

# ==============================
# CONFIGURACOES BANCO
# ==============================

SCHEMA = 'ancore'
TABELA = 'pendencias'

# ==============================
# LEITURA DO SHEETS
# ==============================

def ler_planilha() -> list[list]:
    creds   = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=creds)

    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()

    rows = result.get('values', [])

    # Ignora o cabecalho (primeira linha)
    return rows[1:] if rows else []


# ==============================
# IMPORTACAO PARA O BANCO
# ==============================

def importar(rows: list[list]):
    if not rows:
        print("Nenhum dado encontrado na planilha.")
        return

    query = f"""
        INSERT INTO {SCHEMA}.{TABELA} (
            situacao, qtd_retorno, data, associado,
            placa_chassi, data_cadastro, cooperativa, consultor
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    def cel(row, idx):
        """Retorna o valor da celula ou None se nao existir."""
        try:
            val = row[idx].strip()
            return val if val != '' else None
        except IndexError:
            return None

    tuplas = [
        (
            cel(row, 0),  # situacao
            cel(row, 1),  # qtd_retorno
            cel(row, 2),  # data
            cel(row, 3),  # associado
            cel(row, 4),  # placa_chassi
            cel(row, 5),  # data_cadastro
            cel(row, 6),  # cooperativa
            cel(row, 7),  # consultor
        )
        for row in rows
    ]

    try:
        with psycopg2.connect(
            dbname=dados.db,
            port=os.getenv('DBPORT', '5434'),
            user=dados.user,
            password=dados.password,
            host=dados.host
        ) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, tuplas)

        print(f"[OK] {len(tuplas)} registros importados!")

    except Exception as e:
        print(f"[ERRO] Banco: {e}")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    print("Lendo planilha...")
    rows = ler_planilha()
    print(f"{len(rows)} linhas encontradas.")

    importar(rows)
    print("Concluido.")
