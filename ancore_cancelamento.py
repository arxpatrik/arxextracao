import os
import requests
import dados
import psycopg2
from datetime import date, timedelta
import time


headers = dados.headers_ancore

asc = 'ancore'
emp = 'ancore'

REGISTROS_POR_PAGINA = 100
data_fim    = date.today()
data_inicio = data_fim - timedelta(days=1)


# -------------------------
# QUEBRA EM INTERVALOS
# -------------------------
def gerar_intervalos(data_inicio: date, data_fim: date, max_dias: int = 7):
    atual = data_inicio
    while atual <= data_fim:
        fim_chunk = atual + timedelta(days=max_dias - 1)
        if fim_chunk > data_fim:
            fim_chunk = data_fim
        yield atual, fim_chunk 
        atual = fim_chunk + timedelta(days=1)


# -------------------------
# BUSCAR UMA PÁGINA -------
# -------------------------
def buscar_pagina(url: str, payload: dict, pagina: int, tentativa_label: str) -> tuple[list, bool]:
    payload_paginado = {
        **payload,
        "quantidade_por_pagina": REGISTROS_POR_PAGINA,
        "inicio_paginacao": pagina,
    }

    max_retries = 3
    retry_delay = 15

    for attempt in range(max_retries):
        try:
            print(f"    > Tentativa {attempt+1}/{max_retries} | Pagina {pagina} | {tentativa_label}")

            response = requests.post(
                url,
                headers=headers,
                json=payload_paginado,
                timeout=120
            )

            print(f"    [OK] Status: {response.status_code}")

            if response.status_code != 200:
                print(f"    [ERRO] Resposta inesperada: {response.text[:300]}")

            response.raise_for_status()

            if not response.text.strip():
                print(f"    [ERRO] Resposta vazia.")
                return [], False

            data = response.json()

            if isinstance(data, dict) and data.get("error"):
                print(f"    [ERRO] Erro da API: {data.get('error')}")
                return [], False

            if not isinstance(data, list):
                print(f"    [ERRO] Formato inesperado: {str(data)[:300]}")
                return [], False

            tem_mais = len(data) == REGISTROS_POR_PAGINA
            print(f"    [OK] {len(data)} registros na página {pagina}. {'Ha mais paginas.' if tem_mais else 'Ultima pagina.'}")
            return data, tem_mais

        except requests.exceptions.ConnectTimeout:
            print(f"    [ERRO] Timeout de conexão (servidor não respondeu)")
        except requests.exceptions.ReadTimeout:
            print(f"    [ERRO] Timeout de leitura (servidor conectou mas não retornou dados)")
        except requests.exceptions.ConnectionError as e:
            print(f"    [ERRO] Erro de conexão: {e}")
        except requests.exceptions.HTTPError as e:
            print(f"    [ERRO] Erro HTTP {response.status_code}: {response.text[:300]}")
            return [], False
        except requests.exceptions.RequestException as e:
            print(f"    [ERRO] Erro genérico: {e}")

        if attempt < max_retries - 1:
            print(f"    [...] Aguardando {retry_delay}s antes de tentar novamente...")
            time.sleep(retry_delay)
        else:
            print(f"    [ERRO] Falha definitiva após {max_retries} tentativas. Abortando página.")
            return [], False

    return [], False


# -------------------------
# BUSCAR NA API (COM PAGINAÇÃO)
# -------------------------
def buscar_cancelamentos(inicio: date, fim: date) -> list:
    endpoint = "/listar/alteracao-veiculos"
    url = dados.url + endpoint

    payload = {
        "data_inicial":    inicio.strftime("%d/%m/%Y"),
        "data_final":      fim.strftime("%d/%m/%Y"),
        "valor_anterior":  [1, 10, 4, 16, 18, 23, 17],
        "valor_posterior": [22, 2, 11],
        "campos":          ["codigo_situacao", "placa", "chassi"],
    }

    def to_int(valor):
        try:
            return int(valor) if valor not in (None, "") else None
        except:
            return None

    lista_total = []
    pagina = 0

    while True:
        registros, tem_mais = buscar_pagina(url, payload, pagina, f"{inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}")

        if not registros:
            break

        for x in registros:
            lista_total.append({
                "codigo_alteracao":          x.get("codigo_alteracao"),
                "codigo_veiculo":            x.get("codigo_veiculo"),
                "placa":                     x.get("placa"),
                "chassi":                    x.get("chassi"),
                "valor_anterior":            to_int(x.get("valor_anterior")),
                "valor_posterior":           to_int(x.get("valor_posterior")),
                "data_alteracao":            x.get("data_alteracao"),
                "codigo_usuario_alteracao":  x.get("codigo_usuario_alteracao"),
                "nome_usuario_alteracao":    x.get("nome_usuario_alteracao"),
                "codigo_situacao":           to_int(x.get("valor_posterior")),
                "codigo_motivo_cancelamento": to_int(x.get("codigo_situacaomotivo"))
            })

        if not tem_mais:
            break

        pagina += 1

    print(f"  [OK] Total coletado no chunk: {len(lista_total)} registros.")
    return lista_total


# -------------------------
# IMPORTAR PARA O BANCO
# -------------------------
def importar_cancelamentos(lista: list):
    if not lista:
        print("  [ERRO] Nada para importar.")
        return

    query = f"""
        INSERT INTO {asc}.cancelamentos (
            codigo_alteracao, codigo_veiculo, placa, chassi,
            valor_anterior, valor_posterior,
            data_alteracao, codigo_usuario_alteracao, nome_usuario_alteracao,
            codigo_situacao, codigo_motivo_cancelamento
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (codigo_veiculo, data_alteracao, valor_anterior, valor_posterior) DO NOTHING
    """
    
    try:
        with psycopg2.connect(
            dbname=dados.db,
            port=os.getenv('DBPORT', '5434'),
            user=dados.user,
            password=dados.password,
            host=dados.host
        ) as conn:
            with conn.cursor() as cur:
                tuplas = [
                    (
                        item["codigo_alteracao"],
                        item["codigo_veiculo"],
                        item["placa"],
                        item["chassi"],
                        item["valor_anterior"],
                        item["valor_posterior"],
                        item["data_alteracao"],
                        item["codigo_usuario_alteracao"],
                        item["nome_usuario_alteracao"],
                        item["codigo_situacao"],
                        item["codigo_motivo_cancelamento"]
                    )
                    for item in lista
                ]
                cur.executemany(query, tuplas)

        print(f"  [OK] {len(lista)} registros importados/atualizados!")

    except Exception as e:
        print(f"  [ERRO] Erro Banco: {e}")


# -------------------------
# EXECUÇÃO PRINCIPAL
# -------------------------
def cancelamentos():
    print(f"Periodo: {data_inicio.strftime('%d/%m/%Y')} ate {data_fim.strftime('%d/%m/%Y')}")

    for inicio, fim in gerar_intervalos(data_inicio, data_fim, 7):
        print(f"\nProcessando {inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}...")
        parcial = buscar_cancelamentos(inicio, fim)
        importar_cancelamentos(parcial)


# -------------------------
# RODAR
# -------------------------
if __name__ == "__main__":
    cancelamentos()
    print(f"\n{emp}: Importação concluída!")
