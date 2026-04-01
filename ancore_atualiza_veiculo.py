"""
atualiza_veiculo.py
===================
Script para sincronização de alterações em veículos.

Fluxo:
    1. Coleta os chassis que sofreram alteração via /listar/alteracao-veiculos
       (período de 7 dias retroativos).
    2. Para cada veículo alterado, busca os detalhes completos via /veiculo/buscar
    3. Atualiza os dados na tabela ancore.veiculo.

Campos atualizados:
    placa, codigo_situacao, codigo_cooperativa, data_alteracao, tipo,
    categoria, data_cadastro, data_contrato, nome_voluntario, etc.
"""

import json
import sys
import time
from datetime import date, datetime, timedelta

import psycopg2
import requests

sys.path.append('../associacoes')
import dados

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

SCHEMA_DB            = 'ancore'
TABELA_DB            = 'veiculo'
NOME_EMPRESA         = 'Ancore'
REGISTROS_POR_PAGINA = 3000

# Usar o config do veiculo.py para manter consistência
DB_CONFIG = {
    "dbname":   "postgres",
    "port":     "5434",
    "user":     "arxadm",
    "password": "arx2025",
    "host":     "192.168.0.254",
}

HEADERS_API  = dados.headers_ancore.copy()
HEADERS_API["Content-Type"] = "application/json"
HEADERS_API["User-Agent"]   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

URL_BASE_API = dados.url.rstrip("/")

MAX_TENTATIVAS_502 = 3
ESPERA_502         = 5

# =============================================================================
# REUTILIZANDO LÓGICA DE REQUISIÇÃO
# =============================================================================

def requisitar_com_retry(url: str, payload: dict = None, method: str = "POST", descricao: str = "") -> requests.Response | None:
    for tentativa in range(1, MAX_TENTATIVAS_502 + 1):
        try:
            if method == "POST":
                resposta = requests.post(url, headers=HEADERS_API, data=json.dumps(payload) if payload else None, timeout=90)
            else:
                resposta = requests.get(url, headers=HEADERS_API, timeout=90)

            if resposta.status_code != 502:
                return resposta

            espera = ESPERA_502 * (2 ** (tentativa - 1))
            print(f"  [502] Bad Gateway em '{descricao}' — tentativa {tentativa}/{MAX_TENTATIVAS_502}. Aguardando {espera}s...")
            time.sleep(espera)
        except Exception as e:
            print(f"  [!] Erro de conexão em '{descricao}': {e}.")
            if tentativa == MAX_TENTATIVAS_502:
                return None
            time.sleep(ESPERA_502)
    return None

# =============================================================================
# ETAPA 1 — Coleta de Veículos Alterados
# =============================================================================

def coletar_chassis_alterados(data_inicial: str, data_final: str) -> list[dict]:
    """Busca chassis alterados via /listar/alteracao-veiculos"""
    print(f"[{NOME_EMPRESA}] Buscando alterações de veículo: {data_inicial} até {data_final}...")
    
    chassis_alterados = []
    pagina_atual = 0

    while True:
        payload = {
            "data_inicial": data_inicial,
            "data_final":   data_final,
            "ultima_alteracao": "Y",
            "campos": ["chassi", "placa", "codigo_situacao", "codigo_cooperativa"],
            "inicio_paginacao": pagina_atual,
            "quantidade_por_pagina": REGISTROS_POR_PAGINA
        }

        url = f"{URL_BASE_API}/listar/alteracao-veiculos"
        resposta = requisitar_com_retry(url, payload, descricao=f"Alterações pág {pagina_atual+1}")

        if not resposta or resposta.status_code != 200:
            break

        dados_resp = resposta.json()
        if not dados_resp or not isinstance(dados_resp, list):
            break

        for item in dados_resp:
            chassi = item.get("chassi")
            if chassi:
                chassis_alterados.append({
                    "chassi": chassi,
                    "data_alteracao": item.get("data_alteracao")
                })

        if len(dados_resp) < REGISTROS_POR_PAGINA:
            break
            
        pagina_atual += 1
        time.sleep(1.2) # Firewall protection

    # Remover duplicados mantendo o mais recente (se houver)
    vistos = {}
    for item in chassis_alterados:
        vistos[item["chassi"]] = item["data_alteracao"]
    
    return [{"chassi": k, "data_alteracao": v} for k, v in vistos.items()]

# =============================================================================
# ETAPA 2 — Busca Informação Detalhada
# =============================================================================

def buscar_detalhes_veiculo(chassi: str) -> dict | None:
    """Busca detalhes via /veiculo/buscar/{chassi}/chassi"""
    url = f"{URL_BASE_API}/veiculo/buscar/{chassi}/chassi"
    resposta = requisitar_com_retry(url, method="GET", descricao=f"Detalhes Chassi {chassi}")

    if not resposta or resposta.status_code != 200:
        return None

    dados_resp = resposta.json()
    if isinstance(dados_resp, list) and len(dados_resp) > 0:
        return dados_resp[0]
    return None

# =============================================================================
# ETAPA 3 — Sincronização no Banco
# =============================================================================

def normalizar_valor(valor) -> str:
    """Normaliza valores para comparação, tratando datas (YYYY-MM-DD)."""
    if valor is None:
        return ""
    v_str = str(valor).strip()
    # Tenta capturar apenas a data se o formato for ISO ou similar (YYYY-MM-DD...)
    if len(v_str) >= 10 and v_str[4] == '-' and v_str[7] == '-':
        return v_str[:10]
    return v_str

def atualizar_veiculo_db(cursor, v_api: dict, data_alteracao_api: str) -> bool:
    """Atualiza os campos na tabela ancore.veiculo e loga as mudanças"""
    chassi = v_api.get("chassi")
    
    # Rótulos para o log
    ROTULOS = {
        "codigo_veiculo":     "Cód. Veículo",
        "placa":              "Placa",
        "data_alteracao":     "Alteração",
        "codigo_cooperativa": "Cooperativa",
        "tipo":               "Tipo",
        "categoria":          "Categoria",
        "data_cadastro":      "Cadastro",
        "data_contrato":      "Contrato",
        "codigo_voluntario":  "Cód. Voluntário",
        "nome_voluntario":    "Voluntário",
        "codigo_situacao":    "Situação",
        "codigo_tipo":        "Cód. Tipo",
        "codigo_situacaomotivo": "Motivo Situação",
    }

    try:
        # 1. Busca estado atual
        cursor.execute(f"""
            SELECT codigo_veiculo, placa, data_alteracao, codigo_cooperativa,
                   tipo, categoria, data_cadastro, data_contrato,
                   codigo_voluntario, nome_voluntario, codigo_situacao, codigo_tipo,
                   codigo_situacaomotivo
            FROM {SCHEMA_DB}.{TABELA_DB}
            WHERE chassi = %s
        """, (chassi,))
        
        registro_atual = cursor.fetchone()
        if not registro_atual:
            return False

        # Valores novos (normalizados)
        valores_novos = {
            "codigo_veiculo":     str(v_api.get("codigo_veiculo") or ""),
            "placa":              str(v_api.get("placa") or ""),
            "data_alteracao":     normalizar_valor(data_alteracao_api),
            "codigo_cooperativa": str(v_api.get("codigo_cooperativa") or ""),
            "tipo":               str(v_api.get("tipo") or ""),
            "categoria":          str(v_api.get("categoria") or ""),
            "data_cadastro":      normalizar_valor(v_api.get("data_cadastro")),
            "data_contrato":      normalizar_valor(v_api.get("data_contrato")),
            "codigo_voluntario":  str(v_api.get("codigo_voluntario") or ""),
            "nome_voluntario":    str(v_api.get("nome_voluntario") or ""),
            "codigo_situacao":    str(v_api.get("codigo_situacao") or ""),
            "codigo_tipo":        str(v_api.get("codigo_tipo_veiculo") or ""),
            "codigo_situacaomotivo": str(v_api.get("codigo_situacaomotivo") or ""),
        }

        # Compara
        mudancas = []
        nomes_campos = list(ROTULOS.keys())
        for i, nome in enumerate(nomes_campos):
            valor_antigo = normalizar_valor(registro_atual[i])
            valor_novo   = valores_novos[nome]
            
            if valor_antigo != valor_novo:
                mudancas.append(f"{ROTULOS[nome]}: {valor_antigo} -> {valor_novo}")

        if not mudancas:
            return True # Nada mudou, mas registro existe

        # 2. Executa o UPDATE
        cursor.execute(f"""
            UPDATE {SCHEMA_DB}.{TABELA_DB}
            SET
                codigo_veiculo     = %s,
                placa              = %s,
                data_alteracao     = %s,
                codigo_cooperativa = %s,
                tipo               = %s,
                categoria          = %s,
                data_cadastro      = %s,
                data_contrato      = %s,
                codigo_voluntario  = %s,
                nome_voluntario    = %s,
                codigo_situacao    = %s,
                codigo_tipo        = %s,
                codigo_situacaomotivo = %s
            WHERE chassi = %s
            RETURNING chassi;
        """, (
            valores_novos["codigo_veiculo"] if v_api.get("codigo_veiculo") else None,
            v_api.get("placa"),
            valores_novos["data_alteracao"] if data_alteracao_api else None,
            v_api.get("codigo_cooperativa"),
            v_api.get("tipo"),
            v_api.get("categoria"),
            valores_novos["data_cadastro"] if v_api.get("data_cadastro") else None,
            valores_novos["data_contrato"] if v_api.get("data_contrato") else None,
            v_api.get("codigo_voluntario"),
            v_api.get("nome_voluntario"),
            v_api.get("codigo_situacao"),
            v_api.get("codigo_tipo_veiculo"),
            v_api.get("codigo_situacaomotivo"),
            chassi
        ))
        
        if cursor.fetchone():
            print(f"  [~] VEÍCULO ATUALIZADO: {chassi} | " + " | ".join(mudancas))
            return True
        return False

    except Exception as e:
        print(f"  [!] Erro ao atualizar chassi {chassi}: {e}")
        return False

# =============================================================================
# MAIN
# =============================================================================

def main():
    print(f"\n{'='*60}")
    print(f"  ATUALIZAÇÃO DE VEÍCULOS — {NOME_EMPRESA}")
    print(f"{'='*60}\n")

    hoje = date.today()
    inicio = hoje - timedelta(days=1) # Sincronização diária (ontem para hoje)
    
    data_ini = inicio.strftime("%d/%m/%Y")
    data_fim = hoje.strftime("%d/%m/%Y")

    alterados = coletar_chassis_alterados(data_ini, data_fim)
    
    if not alterados:
        print(f"[{NOME_EMPRESA}] Nenhuma alteração encontrada.")
        return

    print(f"[{NOME_EMPRESA}] Processando detalhes de {len(alterados)} veículo(s)...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(f"[ERRO] Falha na conexão: {e}")
        return

    total_sucesso = 0
    for i, item in enumerate(alterados, 1):
        chassi = item["chassi"]
        data_alt = item["data_alteracao"]
        
        print(f"  [{i}/{len(alterados)}] Sincronizando Chassi: {chassi}...", end="\r")
        
        v_detalhes = buscar_detalhes_veiculo(chassi)
        if v_detalhes:
            if atualizar_veiculo_db(cursor, v_detalhes, data_alt):
                total_sucesso += 1
                conn.commit()
            else:
                conn.rollback()
        
        time.sleep(1.0)

    cursor.close()
    conn.close()

    print(f"\n\n[{NOME_EMPRESA}] Concluído!")
    print(f"  Veículos identificados: {len(alterados)}")
    print(f"  Veículos atualizados:   {total_sucesso}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
