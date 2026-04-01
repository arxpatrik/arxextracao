"""
atualiza_boleto_ancore.py
=========================
Script de atualização diária dos boletos da Ancore.

Fluxo:
    1. Chama /boleto/listar-alteracao com data_inicial=ontem e data_final=hoje
       para coletar os nosso_numero que sofreram alteração.
    2. Para cada nosso_numero coletado, busca os detalhes completos via
       /buscar/boleto (em lotes, para não sobrecarregar a API).
    3. Atualiza os campos disponíveis na resposta do processa-pdf no banco
       (UPDATE por nosso_numero — não faz INSERT, pois o boleto já deve existir
       no banco pela carga histórica do boleto_ancore.py).

Campos atualizados:
    valor_boleto, data_vencimento, situacao_boleto, mes_referente,
    placa, codigo_cooperativa

Campos NÃO disponíveis neste endpoint (não serão alterados):
    chassi, tipo_boleto, data_pagamento, codigo_veiculo,
    codigo_tipo_veiculo, codigo_voluntario, situacao_veiculo
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
NOME_EMPRESA         = 'Ancore'
REGISTROS_POR_PAGINA = 3000
REGISTROS_ALTERACAO  = 1000  # Limite máximo do endpoint listar-alteracao
TAMANHO_LOTE_PDF     = 50     # Qtd de nosso_numero por requisição ao processa-pdf

MODO_DEBUG_SIMULACAO = True   # Se True, apenas lista as alterações no terminal e NÃO SALVA no banco.


HEADERS_API  = dados.headers_ancore
URL_BASE_API = dados.url

MAX_TENTATIVAS_502 = 3   # Tentativas em caso de 502 Bad Gateway
ESPERA_502         = 5   # Segundos de espera base entre tentativas (dobra a cada retry)

MAPA_SITUACAO_BOLETO = {
    '1':   'BAIXADO',
    '2':   'ABERTO',
    '3':   'CANCELADO',
    '4':   'BAIXADO C/ PENDÊNCIA',
    '999': 'EXCLUIDO',
}

REVERSO_SITUACAO_BOLETO = {v: k for k, v in MAPA_SITUACAO_BOLETO.items()}


# =============================================================================
# RETRY PARA 502
# =============================================================================

def requisitar_com_retry(url: str, payload: dict = None, descricao: str = "", method: str = "POST") -> requests.Response | None:
    """
    Faz POST/GET com retry automático em caso de 502 Bad Gateway.
    Aguarda 5s, 10s e 20s entre as tentativas (backoff progressivo).

    Returns:
        Response em caso de sucesso ou qualquer status != 502.
        None se todas as tentativas falharem com 502.
    """
    for tentativa in range(1, MAX_TENTATIVAS_502 + 1):
        if method == "POST":
            resposta = requests.post(url, headers=HEADERS_API, data=json.dumps(payload))
        else:
            resposta = requests.get(url, headers=HEADERS_API)

        if resposta.status_code != 502:
            return resposta

        espera = ESPERA_502 * (2 ** (tentativa - 1))  # 5s, 10s, 20s
        print(
            f"  [502] Bad Gateway em '{descricao}' — tentativa {tentativa}/{MAX_TENTATIVAS_502}. "
            f"Aguardando {espera}s..."
        )
        time.sleep(espera)

    print(f"  [502] Todas as tentativas falharam para '{descricao}'. Pulando.")
    return None

# =============================================================================
# CONEXÃO COM O BANCO
# =============================================================================

def criar_conexao_db() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dbname   = dados.db,
        user     = dados.user,
        password = dados.password,
        host     = dados.host,
        port     = 5434
    )

# =============================================================================
# ETAPA 1 — Coleta de nosso_numero alterados
# =============================================================================

def coletar_nosso_numeros_alterados(data_inicial: str, data_final: str) -> list[str]:
    """
    Chama /boleto/listar-alteracao e retorna todos os nosso_numero
    que sofreram alguma alteração no período informado.

    Args:
        data_inicial: Data no formato DD/MM/YYYY (ontem).
        data_final:   Data no formato DD/MM/YYYY (hoje).

    Returns:
        Lista de nosso_numero (strings) únicos alterados.
    """
    print(f"\n[{NOME_EMPRESA}] Buscando alterações de {data_inicial} a {data_final}...")

    nosso_numeros = []
    pagina_atual  = 0

    while True:
        payload = {
            "data_inicial": data_inicial,
            "data_final":   data_final,
            "campos": [
                # Apenas os campos que efetivamente atualizamos no banco.
                # Monitorar todos os campos gera ruído com alterações que não
                # conseguimos persistir (ex: instrucao, codigo_barras, etc.).
                "codigo_situacao",  # → situacao_boleto
                "data_vencimento",  # → data_vencimento
                "mes_referente",    # → mes_referente
                "valor",            # → valor_boleto
            ],
            "inicio_paginacao":      pagina_atual,
            "quantidade_por_pagina": REGISTROS_ALTERACAO,
        }

        try:
            resposta = requisitar_com_retry(
                URL_BASE_API + "/boleto/listar-alteracao",
                payload,
                f"listar-alteracao pág {pagina_atual + 1}"
            )

            if resposta is None:
                print(f"[{NOME_EMPRESA}] Falha permanente na página {pagina_atual + 1}. Encerrando coleta.")
                break

            # 406 = nenhuma alteração encontrada (comportamento esperado)
            if resposta.status_code == 406:
                mensagem = str(resposta.text).lower()
                if "não foram encontradas" in mensagem or "nenhum registro" in mensagem:
                    print(f"[{NOME_EMPRESA}] Nenhuma alteração encontrada para o período.")
                    break
                print(f"[{NOME_EMPRESA}] Erro 406 inesperado: {resposta.text[:300]}")
                break

            if resposta.status_code != 200:
                print(f"[{NOME_EMPRESA}] Erro HTTP {resposta.status_code}: {resposta.text[:300]}")
                break

            dados_resposta = resposta.json()
            total_paginas  = dados_resposta.get("numero_paginas", 0)
            resultado      = dados_resposta.get("resultado", [])

            if total_paginas == 0 or not resultado:
                print(f"[{NOME_EMPRESA}] Sem dados retornados na página {pagina_atual}.")
                break

            for item in resultado:
                nn = item.get("nosso_numero")
                if nn:
                    nosso_numeros.append(str(nn))

            print(f"[{NOME_EMPRESA}] Página {pagina_atual + 1}/{total_paginas} — {len(resultado)} alterações coletadas.")

            pagina_atual += 1
            if pagina_atual >= total_paginas:
                break

            # Respeita limite de 1 req/s do firewall da Ancore
            time.sleep(1.0)

        except Exception as erro:
            print(f"[{NOME_EMPRESA}] Exceção ao coletar alterações: {erro}")
            break

    # Remove duplicatas mantendo a ordem
    vistos = set()
    unicos = []
    for nn in nosso_numeros:
        if nn not in vistos:
            vistos.add(nn)
            unicos.append(nn)

    print(f"[{NOME_EMPRESA}] Total de nosso_numero únicos alterados: {len(unicos)}")
    return unicos

# =============================================================================
# ETAPA 2 — Busca de detalhes via processa-pdf/boleto
# =============================================================================

def buscar_detalhes_boletos(lista_nosso_numero: list[str]) -> list[dict]:
    """
    Chama /buscar/boleto/{nosso_numero} individualmente e retorna os
    dados normalizados prontos para UPDATE no banco, desmembrando
    cada chassi como um registro único.
    """
    boletos_normalizados = []
    total = len(lista_nosso_numero)

    for i, nosso_numero in enumerate(lista_nosso_numero, 1):
        try:
            url_endpoint = f"{URL_BASE_API}/buscar/boleto/{nosso_numero}"
            resposta = requisitar_com_retry(
                url_endpoint,
                method="GET",
                descricao=f"boleto {i}/{total} ({nosso_numero})"
            )

            if resposta is None:
                continue

            if resposta.status_code != 200:
                print(f"[{NOME_EMPRESA}] Erro HTTP {resposta.status_code} ao buscar {nosso_numero}: {resposta.text[:100]}")
                time.sleep(1.0)
                continue

            boleto_api = resposta.json()
            if not boleto_api:
                continue

            mes_referente_raw = boleto_api.get("mes_referente", "")
            try:
                mes_referente = datetime.strptime(mes_referente_raw, "%m/%Y").date() if mes_referente_raw else None
            except ValueError:
                mes_referente = None

            cod_situacao = boleto_api.get("codigo_situacao_boleto")
            if not cod_situacao:
               desc_situacao = boleto_api.get("descricao_situacao_boleto", "").upper()
               cod_situacao = REVERSO_SITUACAO_BOLETO.get(desc_situacao, desc_situacao)

            data_pagamento_raw = boleto_api.get("data_pagamento", "")
            data_pagamento = data_pagamento_raw if data_pagamento_raw and data_pagamento_raw != "0000-00-00" else None

            veiculos = boleto_api.get("veiculos", [])
            for v in veiculos:
                chassi = v.get("chassi")
                if not chassi:
                    continue

                boletos_normalizados.append({
                    "nosso_numero":        str(nosso_numero),
                    "valor_boleto":        boleto_api.get("valor_boleto"),
                    "data_vencimento":     boleto_api.get("data_vencimento"),
                    "situacao_boleto":     cod_situacao,
                    "mes_referente":       mes_referente,
                    "tipo_boleto":         boleto_api.get("codigo_tipo_boleto"),
                    "data_pagamento":      data_pagamento,
                    "placa":               v.get("placa"),
                    "chassi":              chassi,
                    "codigo_veiculo":      v.get("codigo_veiculo"),
                    "situacao_veiculo":    v.get("codigo_situacao_veiculo"),
                    "codigo_tipo_veiculo": v.get("codigo_tipo_veiculo"),
                    "codigo_cooperativa":  v.get("codigo_cooperativa_veiculo"),
                })

            time.sleep(1.0)  # Firewall Ancore

        except Exception as erro:
            print(f"[{NOME_EMPRESA}] Exceção ao buscar nosso_numero {nosso_numero}: {erro}")
            time.sleep(1.0)
            continue

    return boletos_normalizados


# =============================================================================
# ETAPA 3 — UPDATE no banco
# =============================================================================

def atualizar_boletos_banco(cursor, conn, lista_boletos: list[dict]) -> tuple[int, int]:
    """
    Atualiza os campos disponíveis dos boletos no banco, identificando-os
    pelo nosso_numero.

    Antes de cada UPDATE, busca os valores atuais do registro para detectar
    e registrar no log apenas o que efetivamente mudou.

    Não faz INSERT — o boleto já deve existir no banco pelo boleto_ancore.py.
    Campos não disponíveis no processa-pdf/boleto são preservados intactos.

    Returns:
        (total_atualizados, total_nao_encontrados)
    """
    total_atualizados     = 0
    total_ja_atualizados  = 0   # encontrado no banco mas sem mudança real
    total_nao_encontrados = 0

    # Rótulos legíveis para o log
    ROTULOS = {
        "valor_boleto":       "Valor",
        "data_vencimento":    "Venc.",
        "situacao_boleto":    "Sit. Boleto",
        "mes_referente":      "Mês Ref.",
        "data_pagamento":     "Pagto.",
        "placa":              "Placa",
        "codigo_cooperativa": "Coop.",
        "situacao_veiculo":   "Sit. Veículo",
        "tipo_boleto":        "Tipo Boleto",
    }
    nomes_campos = list(ROTULOS.keys())

    for boleto in lista_boletos:
        try:
            # ── 1. Busca estado atual antes de atualizar ──────────────────────
            cursor.execute(f"""
                SELECT valor_boleto, data_vencimento, situacao_boleto,
                       mes_referente, data_pagamento, placa, codigo_cooperativa,
                       situacao_veiculo, tipo_boleto
                FROM {SCHEMA_DB}.boleto_ancore
                WHERE nosso_numero = %s AND chassi = %s
                LIMIT 1
            """, (boleto['nosso_numero'], boleto['chassi']))


            registro_atual = cursor.fetchone()

            if registro_atual is None:
                # Boleto ainda não está no banco — esperado durante carga histórica
                if MODO_DEBUG_SIMULACAO:
                    print(
                        f"  [SIMULAÇÃO] BOLETO NÃO ENCONTRADO NO BANCO: {boleto['nosso_numero']} | "
                        f"Situação API: {boleto['situacao_boleto']} | Vencimento API: {boleto['data_vencimento']} | Valor API: {boleto['valor_boleto']}"
                    )
                total_nao_encontrados += 1
                continue

            # ── 2. Compara e loga o que mudou ─────────────────────────────────
            campos_novos = [
                boleto['valor_boleto'],
                boleto['data_vencimento'],
                boleto['situacao_boleto'],
                boleto['mes_referente'],
                boleto['data_pagamento'],
                boleto['placa'],
                boleto['codigo_cooperativa'],
                boleto['situacao_veiculo'],
                boleto['tipo_boleto'],
            ]

            mudancas = []
            for nome, antigo, novo in zip(nomes_campos, registro_atual, campos_novos):
                if novo is None:
                    continue  # COALESCE manteria o valor antigo, sem mudança
                if str(antigo) != str(novo):
                    mudancas.append(
                        f"{ROTULOS[nome]}: {antigo} → {novo}"
                    )

            if mudancas:
                status_alteracao = "[SIMULAÇÃO] BOLETO MUDARIA" if MODO_DEBUG_SIMULACAO else "[~] BOLETO ATUALIZADO"
                print(
                    f"  {status_alteracao}: {boleto['nosso_numero']} (Chassi {boleto['chassi']}) | "
                    + " | ".join(mudancas)
                )
                total_atualizados += 1

                if not MODO_DEBUG_SIMULACAO:
                    # ── 3. Executa o UPDATE (Apenas se houver mudança) ──────────────────
                    cursor.execute(f"""
                        UPDATE {SCHEMA_DB}.boleto_ancore
                        SET
                            valor_boleto       = COALESCE(%s, valor_boleto),
                            data_vencimento    = COALESCE(%s, data_vencimento),
                            situacao_boleto    = COALESCE(%s, situacao_boleto),
                            mes_referente      = COALESCE(%s, mes_referente),
                            data_pagamento     = COALESCE(%s, data_pagamento),
                            placa              = COALESCE(%s, placa),
                            codigo_cooperativa = COALESCE(%s, codigo_cooperativa),
                            situacao_veiculo   = COALESCE(%s, situacao_veiculo),
                            tipo_boleto        = COALESCE(%s, tipo_boleto),
                            codigo_veiculo     = COALESCE(%s, codigo_veiculo),
                            codigo_tipo_veiculo= COALESCE(%s, codigo_tipo_veiculo)
                        WHERE nosso_numero = %s AND chassi = %s
                    """, (
                        boleto['valor_boleto'],
                        boleto['data_vencimento'],
                        boleto['situacao_boleto'],
                        boleto['mes_referente'],
                        boleto['data_pagamento'],
                        boleto['placa'],
                        boleto['codigo_cooperativa'],
                        boleto['situacao_veiculo'],
                        boleto['tipo_boleto'],
                        boleto['codigo_veiculo'],
                        boleto['codigo_tipo_veiculo'],
                        boleto['nosso_numero'],
                        boleto['chassi'],
                    ))
            else:
                total_ja_atualizados += 1

        except Exception as erro:
            print(f"  [!] Erro ao atualizar nosso_numero {boleto['nosso_numero']} chassi {boleto['chassi']}: {erro}")

            conn.rollback()

    conn.commit()
    return total_atualizados, total_ja_atualizados, total_nao_encontrados

# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

def main() -> None:
    print(f"\n{'='*60}")
    print(f"  ATUALIZAÇÃO DIÁRIA DE BOLETOS — {NOME_EMPRESA}")
    print(f"{'='*60}\n")

    hoje   = date.today()
    ontem  = hoje - timedelta(days=1)
    #formatação para o formato que a API espera: dd/mm/yyyy
    data_inicial = ontem.strftime("%d/%m/%Y")
    data_final   = hoje.strftime("%d/%m/%Y")

    print(f"Período de alterações: {data_inicial} → {data_final}\n")

    # ── Etapa 1: coleta nos nosso_numeros alterados ───────────────────────────
    nosso_numeros = coletar_nosso_numeros_alterados(data_inicial, data_final)

    if not nosso_numeros:
        print(f"\n[{NOME_EMPRESA}] Nenhuma alteração a processar. Encerrando.")
        return

    # ── Etapa 2: busca detalhes completos ────────────────────────────────────
    print(f"\n[{NOME_EMPRESA}] Buscando detalhes de {len(nosso_numeros)} boleto(s) via processa-pdf...")
    boletos = buscar_detalhes_boletos(nosso_numeros)
    print(f"[{NOME_EMPRESA}] {len(boletos)} boleto(s) com detalhes obtidos.\n")

    if not boletos:
        print(f"[{NOME_EMPRESA}] Nenhum detalhe obtido. Encerrando.")
        return

    # ── Etapa 3: atualiza banco ───────────────────────────────────────────────
    try:
        conn   = criar_conexao_db()
        cursor = conn.cursor()
    except Exception as erro:
        print(f"[ERRO CRÍTICO] Não foi possível conectar ao banco: {erro}")
        sys.exit(1)

    atualizados, ja_atualizados, nao_encontrados = atualizar_boletos_banco(cursor, conn, boletos)

    cursor.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"  RESULTADO:")
    print(f"  Alterados        : {atualizados}")
    print(f"  Já atualizados   : {ja_atualizados}  ← encontrados, sem mudança")
    print(f"  Não encontrados  : {nao_encontrados}  ← fora do banco (carga histórica)")
    print(f"  ATUALIZAÇÃO {NOME_EMPRESA} CONCLUÍDA")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
