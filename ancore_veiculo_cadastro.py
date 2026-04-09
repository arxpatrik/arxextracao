"""
ETL - Extracao e importacao de veiculos por data de cadastro.
Destino: ancore.veiculo
Periodo: dia anterior ate hoje (execucao diaria)
"""

import time
import requests
import psycopg2
import dados
from datetime import date, timedelta

# ==============================
# CONFIGURACOES
# ==============================

URL_BASE = dados.url.rstrip("/") + "/listar/veiculo"

HEADERS = dados.headers_ancore.copy()
HEADERS["User-Agent"]   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
HEADERS["Content-Type"] = "application/json"

SCHEMA = "ancore"
TABELA = "veiculo"

DB_CONFIG = {
    "dbname":   "postgres",
    "port":     "5434",
    "user":     "arxadm",
    "password": "arx2025",
    "host":     "192.168.0.254",
}

hoje  = date.today()
ontem = hoje - timedelta(days=120)

# Configuracoes de paginacao e retry
QTD_POR_PAGINA  = 5000  # maximo seguro (API aceita ate 5000)
MAX_TENTATIVAS  = 5     # tentativas por pagina antes de desistir
SLEEP_ENTRE_PAGINAS = 1.2   # segundos entre cada requisicao
SLEEP_RETRY         = 10.0  # segundos de espera apos timeout


# ==============================
# BANCO DE DADOS
# ==============================

def salvar_lote(conn, lista_veiculos: list[dict]) -> None:
    if not lista_veiculos:
        return

    tuplas = []
    for v in lista_veiculos:
        tuplas.append((
            v.get("chassi"),
            v.get("codigo_veiculo"),
            v.get("placa"),
            v.get("data_alteracao"),
            v.get("codigo_cooperativa"),
            v.get("mes_referente"),
            v.get("tipo"),
            v.get("categoria"),
            v.get("usuario_cadastro"),
            v.get("data_cadastro"),
            v.get("data_contrato"),
            v.get("codigo_voluntario"),
            v.get("nome_voluntario"),
            v.get("codigo_situacao"),
            v.get("codigo_vencimento"),
            v.get("codigo_tipo"),
        ))

    query = f"""
        INSERT INTO {SCHEMA}.{TABELA} (
            chassi,
            codigo_veiculo,
            placa,
            data_alteracao,
            codigo_cooperativa,
            mes_referente,
            tipo,
            categoria,
            usuario_cadastro,
            data_cadastro,
            data_contrato,
            codigo_voluntario,
            nome_voluntario,
            codigo_situacao,
            codigo_vencimento,
            codigo_tipo
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (chassi) DO UPDATE SET
            codigo_veiculo     = EXCLUDED.codigo_veiculo,
            placa              = EXCLUDED.placa,
            data_alteracao     = EXCLUDED.data_alteracao,
            codigo_cooperativa = EXCLUDED.codigo_cooperativa,
            mes_referente      = EXCLUDED.mes_referente,
            tipo               = EXCLUDED.tipo,
            categoria          = EXCLUDED.categoria,
            usuario_cadastro   = EXCLUDED.usuario_cadastro,
            data_cadastro      = EXCLUDED.data_cadastro,
            data_contrato      = EXCLUDED.data_contrato,
            codigo_voluntario  = EXCLUDED.codigo_voluntario,
            nome_voluntario    = EXCLUDED.nome_voluntario,
            codigo_situacao    = EXCLUDED.codigo_situacao,
            codigo_vencimento  = EXCLUDED.codigo_vencimento,
            codigo_tipo        = EXCLUDED.codigo_tipo
    """

    with conn.cursor() as cur:
        cur.executemany(query, tuplas)
    conn.commit()


# ==============================
# API - BUSCA DE UMA PAGINA
# ==============================

def buscar_pagina(inicio_pag: int) -> list[dict] | None:
    payload = {
        "data_cadastro_inicial": ontem.strftime("%d/%m/%Y"),
        "data_cadastro_final":   hoje.strftime("%d/%m/%Y"),
        "inicio_paginacao":      inicio_pag,
        "quantidade_por_pagina": QTD_POR_PAGINA,
    }

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            response = requests.post(
                URL_BASE,
                headers=HEADERS,
                json=payload,
                timeout=90,
            )
            response.raise_for_status()
            return response.json().get("veiculos", [])

        except requests.exceptions.ReadTimeout:
            print(f"Timeout (offset {inicio_pag}) — tentativa {tentativa}/{MAX_TENTATIVAS}")
            time.sleep(SLEEP_RETRY)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP {response.status_code} (offset {inicio_pag}): {e}")
            return None

        except requests.exceptions.RequestException as e:
            print(f"Erro de requisicao (tentativa {tentativa}/{MAX_TENTATIVAS}): {e}")
            time.sleep(SLEEP_RETRY)

    print(f"API nao respondeu apos {MAX_TENTATIVAS} tentativas. Abortando.")
    return None


# ==============================
# ORQUESTRACAO
# ==============================

def carga_por_cadastro(conn) -> None:
    inicio_pag = 0
    total      = 0

    print(f"\nIniciando | data_cadastro: {ontem.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}")

    while True:
        time.sleep(SLEEP_ENTRE_PAGINAS)

        lista_veiculos = buscar_pagina(inicio_pag)

        if lista_veiculos is None:
            break

        print(f"   Offset {inicio_pag:>6} -> {len(lista_veiculos)} registros")

        if not lista_veiculos:
            break

        try:
            salvar_lote(conn, lista_veiculos)
        except Exception as e:
            print(f"   Erro ao salvar lote (offset {inicio_pag}): {e}")
            break

        total      += len(lista_veiculos)
        inicio_pag += QTD_POR_PAGINA

        if len(lista_veiculos) < QTD_POR_PAGINA:
            break

    print(f"   Total sincronizado: {total}")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    print("=" * 50)
    print("ETL - veiculo_cadastro | Iniciando")
    print("=" * 50)

    with psycopg2.connect(**DB_CONFIG) as conn:
        carga_por_cadastro(conn)

    print("\nExtracao e importacao concluidas.")
