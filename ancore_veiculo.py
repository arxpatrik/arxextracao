"""
ETL - Extração e importação de veículos por situação.
Destino: ancore.veiculo
"""

import time
import requests
import psycopg2
import dados

# ==============================
# CONFIGURAÇÕES
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

# Todos os códigos de situação conhecidos pela API
SITUACOES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
             11, 12, 13, 14, 15, 16, 17, 18,
             19, 20, 21, 22, 23, 24]

# Configurações de paginação e retry
QTD_POR_PAGINA = 5000   # máximo seguro (API aceita até 5000)
MAX_TENTATIVAS = 5      # tentativas por página antes de desistir
SLEEP_ENTRE_PAGINAS = 1.2   # segundos entre cada requisição
SLEEP_RETRY         = 10.0  # segundos de espera após timeout


# ==============================
# BANCO DE DADOS
# ==============================

def salvar_lote(conn, lista_veiculos: list[dict]) -> None:
    """
    Insere ou atualiza um lote de veículos na tabela veiculo_raw.
    O chassi é a PK — em conflito, todos os outros campos são atualizados.
    """
    if not lista_veiculos:
        return

    # Monta as tuplas respeitando a ordem das colunas do INSERT abaixo
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
# API — BUSCA DE UMA PÁGINA
# ==============================

def buscar_pagina(codigo_situacao: int, inicio_pag: int) -> list[dict] | None:
    """
    Faz POST na API para uma situação e offset específicos.
    Retorna a lista de veículos ou None se todas as tentativas falharem.
    """
    payload = {
        "codigo_situacao":       codigo_situacao,
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
            print(f"⏳ Timeout (situação {codigo_situacao}, offset {inicio_pag}) "
                  f"— tentativa {tentativa}/{MAX_TENTATIVAS}")
            time.sleep(SLEEP_RETRY)

        except requests.exceptions.HTTPError as e:
            print(f"⚠️  HTTP {response.status_code} (situação {codigo_situacao}): {e}")
            return None

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro de requisição (tentativa {tentativa}/{MAX_TENTATIVAS}): {e}")
            time.sleep(SLEEP_RETRY)

    print(f"❌ API não respondeu após {MAX_TENTATIVAS} tentativas. Pulando situação {codigo_situacao}.")
    return None


# ==============================
# ORQUESTRAÇÃO POR SITUAÇÃO
# ==============================

def carga_por_situacao(conn, codigo_situacao: int) -> None:
    """
    Pagina a API para uma situação e persiste cada lote no banco.
    Para quando a API retorna lista vazia ou menor que QTD_POR_PAGINA.
    """
    inicio_pag = 0
    total      = 0

    print(f"\n🚀 Iniciando | codigo_situacao = {codigo_situacao}")

    while True:
        # Respeita intervalo entre páginas para não sobrecarregar a API
        time.sleep(SLEEP_ENTRE_PAGINAS)

        lista_veiculos = buscar_pagina(codigo_situacao, inicio_pag)

        # None = falha total na API, abandona essa situação
        if lista_veiculos is None:
            break

        print(f"   📡 Offset {inicio_pag:>6} → {len(lista_veiculos)} registros")

        # Lista vazia = não há mais páginas
        if not lista_veiculos:
            break

        try:
            salvar_lote(conn, lista_veiculos)
        except Exception as e:
            print(f"   ❌ Erro ao salvar lote (offset {inicio_pag}): {e}")
            break

        total      += len(lista_veiculos)
        inicio_pag += QTD_POR_PAGINA

        # Última página — menos registros do que o solicitado
        if len(lista_veiculos) < QTD_POR_PAGINA:
            break

    print(f"   📊 Total sincronizado (situação {codigo_situacao}): {total}")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    print("=" * 50)
    print("ETL - veiculo | Iniciando")
    print("=" * 50)

    # Uma única conexão é aberta para todo o processo
    with psycopg2.connect(**DB_CONFIG) as conn:
        for situacao in SITUACOES:
            carga_por_situacao(conn, situacao)

    print("\n✅ Extração e importação concluídas.")