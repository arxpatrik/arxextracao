import os
import requests
import psycopg2
import dados
from datetime import date, timedelta

# ============================================================
# CONFIGURACOES
# ============================================================

headers = dados.headers_ancore
asc     = 'ancore'
emp     = 'Ancore'
url     = dados.url.rstrip("/") + "/listar/historico-atendimento-associado"

codes_ancore    = [100140, 100271]
QTD_POR_PAGINA  = 5000
DIAS_PARA_TRAS  = 1

# ============================================================
# GERACAO DE DATAS
# ============================================================

def obter_datas(dias_para_tras: int) -> list[str]:
    hoje = date.today()
    return [
        (hoje - timedelta(days=i)).strftime("%d/%m/%Y")
        for i in range(dias_para_tras + 1)
    ]

# ============================================================
# BUSCA E PAGINACAO
# ============================================================

def retention():
    datas = obter_datas(DIAS_PARA_TRAS)

    for data in datas:
        print(f"\n{'-'*50}")
        print(f"Buscando retencoes para o dia {data}")
        print(f"{'-'*50}")

        for codigo_atual in codes_ancore:
            inicio_paginacao = 0

            while True:
                payload = {
                    "codigo_tipo_atendimento": codigo_atual,
                    "data_cadastro_inicial":   data,
                    "data_cadastro_final":     data,
                    "inicio_paginacao":        inicio_paginacao,
                    "quantidade_por_pagina":   QTD_POR_PAGINA,
                }

                try:
                    response = requests.post(url, headers=headers, json=payload, timeout=60)
                    response.raise_for_status()

                    data_resp = response.json()

                    if not isinstance(data_resp, list):
                        print(f"{emp}: Codigo {codigo_atual} -> Sem registros ou resposta inesperada.")
                        break

                    registros = [
                        {
                            "id_retencao":      item.get("codigo_atendimento"),
                            "cod_veiculo":      item.get("codigo_veiculo"),
                            "tipo_atendimento": item.get("titulo"),
                            "data_atendimento": item.get("data_cadastro"),
                            "status":           item.get("codigo_status_atendimento"),
                        }
                        for item in data_resp
                    ]

                    if registros:
                        print(f"{emp}: Codigo {codigo_atual} (Pagina {inicio_paginacao}) -> {len(registros)} registros.")
                        import_ret(registros)

                    if len(registros) < QTD_POR_PAGINA:
                        break

                    inicio_paginacao += 1

                except requests.exceptions.RequestException as e:
                    print(f"{emp}: Erro de rede — codigo {codigo_atual} (Pagina {inicio_paginacao}): {e}")
                    break
                except Exception as e:
                    print(f"{emp}: Erro inesperado -> {e}")
                    break

# ============================================================
# PERSISTENCIA NO BANCO
# ============================================================

def import_ret(lista_ret: list):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=dados.db,
            port=os.getenv('DBPORT', '5434'),
            user=dados.user,
            password=dados.password,
            host=dados.host,
        )
        cur = conn.cursor()

        cur.executemany(
            f"""
            INSERT INTO {asc}.retencao (id_retencao, cod_veiculo, tipo_atendimento, data_atendimento, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_retencao) DO UPDATE SET status = EXCLUDED.status
            """,
            [
                (i['id_retencao'], i['cod_veiculo'], i['tipo_atendimento'], i['data_atendimento'], i['status'])
                for i in lista_ret
            ]
        )

        conn.commit()
        print(f"{emp}: {len(lista_ret)} retencoes salvas.")

    except psycopg2.DatabaseError as e:
        print(f"Erro no banco: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == '__main__':
    retention()
