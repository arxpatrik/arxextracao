"""
extrator_boletos_ancore.py
==========================
Script de extração e sincronização de boletos da API da Ancore com o banco de dados PostgreSQL.

Fluxo principal:
    1. Gera uma lista de meses (primeiro e último dia de cada mês) para varredura.
    2. Para cada mês:
        a. Busca todos os boletos emitidos naquele mês via API.
        b. Realiza upsert (insert ou update) dos registros no banco de dados.

Dependências externas:
    - requests
    - psycopg2
    - dados (módulo local com credenciais e configurações)
"""

import json
import sys
import time
from calendar import monthrange
from datetime import date, datetime

import psycopg2
import requests

sys.path.append('../associacoes')
import dados

# =============================================================================
# CONFIGURAÇÕES GERAIS
# =============================================================================

SCHEMA_DB            = 'ancore'   # Schema do PostgreSQL onde os dados serão gravados
NOME_EMPRESA         = 'Ancore'   # Nome exibido nos logs

# Mês/ano de início da carga histórica
ANO_INICIO           = 2025       # Ano de início
MES_INICIO           = 5          # Mês de início

# Paginação
REGISTROS_POR_PAGINA = 3000       # Quantidade de registros retornados por página da API

# Mapeamento de códigos de situação da API → rótulos legíveis
MAPA_SITUACAO_BOLETO = {
    '1':   'BAIXADO',
    '2':   'ABERTO',
    '3':   'CANCELADO',
    '4':   'BAIXADO C/ PENDÊNCIA',
    '999': 'EXCLUIDO',
}

HEADERS_API  = dados.headers_ancore
URL_BASE_API = dados.url

MAX_TENTATIVAS_502 = 3   # Tentativas em caso de 502 Bad Gateway
ESPERA_502         = 5   # Segundos de espera base entre tentativas (dobra a cada retry)

# =============================================================================
# RETRY PARA 502 — TRATATIVA DE BAD GATEWAY
# =============================================================================

def requisitar_com_retry(url: str, payload: dict, descricao: str = "") -> requests.Response | None:
    """
    Faz POST com retry automático em caso de 502 Bad Gateway.
    Aguarda 5s, 10s e 20s entre as tentativas (backoff progressivo).

    Returns:
        Response em caso de sucesso ou qualquer status != 502.
        None se todas as tentativas falharem com 502.
    """
    for tentativa in range(1, MAX_TENTATIVAS_502 + 1):
        try:
            resposta = requests.post(url, headers=HEADERS_API, data=json.dumps(payload))

            if resposta.status_code != 502:
                return resposta

            espera = ESPERA_502 * (2 ** (tentativa - 1))  # 5s, 10s, 20s
            print(
                f"  [502] Bad Gateway em '{descricao}' — tentativa {tentativa}/{MAX_TENTATIVAS_502}. "
                f"Aguardando {espera}s..."
            )
            time.sleep(espera)
        except Exception as e:
            print(f"  [!] Erro de conexão em '{descricao}': {e}. Tentativa {tentativa}/{MAX_TENTATIVAS_502}...")
            if tentativa == MAX_TENTATIVAS_502:
                raise e
            time.sleep(ESPERA_502 * (2 ** (tentativa - 1)))

    print(f"  [502] Todas as tentativas falharam para '{descricao}'.")
    return None

# =============================================================================
# CONEXÃO COM O BANCO DE DADOS
# =============================================================================

def criar_conexao_db() -> psycopg2.extensions.connection:
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL
    usando as credenciais definidas no módulo `dados`.

    Returns:
        psycopg2.extensions.connection: Objeto de conexão ativa.

    Raises:
        psycopg2.OperationalError: Se não for possível conectar.
    """
    return psycopg2.connect(
        dbname   = dados.db,
        user     = dados.user,
        password = dados.password,
        host     = dados.host,
        port     = 5434
    )

# =============================================================================
# GERAÇÃO DE MESES
# =============================================================================

def gerar_meses(ano_inicio: int, mes_inicio: int) -> list[tuple[str, str]]:
    """
    Gera lista de tuplas (data_inicio, data_fim) mês a mês no formato DD/MM/YYYY.

    Cada tupla representa o primeiro e o último dia do mês, garantindo blocos
    exatos que respeitam o limite de 31 dias da API. Como a busca é por
    data_emissao, não faz sentido buscar datas futuras — o loop vai apenas
    até o mês atual.

    Args:
        ano_inicio: Ano de início da varredura (ex: 2024)
        mes_inicio: Mês de início da varredura (ex: 1)

    Returns:
        Lista de tuplas (data_inicio, data_fim) no formato DD/MM/YYYY

    Exemplo:
        >>> gerar_meses(2024, 11)
        [('01/11/2024', '30/11/2024'), ('01/12/2024', '31/12/2024'), ...]
    """
    hoje  = date.today()
    meses = []
    ano, mes = ano_inicio, mes_inicio

    while (ano, mes) <= (hoje.year, hoje.month):
        ultimo_dia  = monthrange(ano, mes)[1]
        data_inicio = f"01/{mes:02d}/{ano}"
        data_fim    = f"{ultimo_dia:02d}/{mes:02d}/{ano}"
        meses.append((data_inicio, data_fim))

        mes += 1
        if mes > 12:
            mes = 1
            ano += 1

    return meses

# =============================================================================
# UPSERT DE BOLETOS
# =============================================================================

def upsert_boletos(cursor, lista_boletos: list[dict]) -> tuple[int, int]:
    """
    Insere ou atualiza boletos no banco de dados (upsert via ON CONFLICT).
    """
    total_inseridos   = 0
    total_atualizados = 0

    for boleto in lista_boletos:
        cursor.execute(
            f"SELECT situacao_boleto, data_pagamento FROM {SCHEMA_DB}.boleto_ancore WHERE chassi = %s AND nosso_numero = %s",
            (boleto['chassi'], boleto['nosso_numero'])
        )
        registro_existente = cursor.fetchone()

        cursor.execute(f"""
            INSERT INTO {SCHEMA_DB}.boleto_ancore(
                nosso_numero, valor_boleto, tipo_boleto, data_vencimento,
                data_pagamento, situacao_boleto, mes_referente,
                codigo_veiculo, codigo_tipo_veiculo, codigo_cooperativa,
                codigo_voluntario, placa, chassi, situacao_veiculo
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chassi, nosso_numero)
            DO UPDATE SET
                nosso_numero        = EXCLUDED.nosso_numero,
                valor_boleto        = EXCLUDED.valor_boleto,
                tipo_boleto         = EXCLUDED.tipo_boleto,
                data_vencimento     = EXCLUDED.data_vencimento,
                data_pagamento      = EXCLUDED.data_pagamento,
                situacao_boleto     = EXCLUDED.situacao_boleto,
                codigo_veiculo      = EXCLUDED.codigo_veiculo,
                codigo_tipo_veiculo = EXCLUDED.codigo_tipo_veiculo,
                codigo_cooperativa  = EXCLUDED.codigo_cooperativa,
                codigo_voluntario   = EXCLUDED.codigo_voluntario,
                placa               = EXCLUDED.placa,
                situacao_veiculo    = EXCLUDED.situacao_veiculo
            RETURNING xmax;
        """, (
            boleto['nosso_numero'],
            boleto['valor_boleto'],
            boleto['tipo_boleto'],
            boleto['data_vencimento'],
            boleto['data_pagamento'],
            boleto['situacao_boleto'],
            boleto['mes_referente'],
            boleto['codigo_veiculo'],
            boleto['codigo_tipo_veiculo'],
            boleto['codigo_cooperativa'],
            boleto['codigo_voluntario'],
            boleto['placa'],
            boleto['chassi'],
            boleto['situacao_veiculo']
        ))

        resultado = cursor.fetchone()
        if not resultado:
            continue

        # xmax != '0' significa que houve UPDATE (não INSERT)
        foi_atualizado = str(resultado[0]) != '0'

        if not foi_atualizado:
            print(
                f"  [+] NOVO BOLETO: Chassi {boleto['chassi']} - NOSSO_NUMERO: {boleto['nosso_numero']} | "
                f"Situação: {boleto['situacao_boleto']} | "
                f"Venc: {boleto['data_vencimento']}"
            )
            total_inseridos += 1

        elif registro_existente:
            situacao_antiga, pagamento_antigo = registro_existente
            houve_mudanca = (
                situacao_antiga != boleto['situacao_boleto'] or
                str(pagamento_antigo) != str(boleto['data_pagamento'])
            )
            if houve_mudanca:
                print(
                    f"  [~] BOLETO ATUALIZADO: {boleto['nosso_numero']} | "
                    f"Situação: {situacao_antiga} → {boleto['situacao_boleto']} | "
                    f"Pagamento: {pagamento_antigo} → {boleto['data_pagamento']}"
                )
                total_atualizados += 1

    return total_inseridos, total_atualizados

# =============================================================================
# UPSERT DE ALTERAÇÕES DE SITUAÇÃO
# =============================================================================

def upsert_alteracoes_de_situacao(cursor, lista_alteracoes: list[dict]) -> int:
    """
    Aplica alterações de situação em boletos já existentes no banco de dados.

    Utilizado para sincronizar eventos como cancelamentos, exclusões e baixas
    externas que chegam pela rota de alterações da API (diferente da rota
    principal de listagem).

    Args:
        cursor:           Cursor psycopg2 com transação ativa.
        lista_alteracoes: Lista de dicts com 'numero_boleto' e 'situacao_boleto'.

    Returns:
        Total de registros efetivamente alterados no banco.
    """
    total_alterados = 0

    for alteracao in lista_alteracoes:
        cursor.execute(
            f"SELECT situacao_boleto FROM {SCHEMA_DB}.boleto_ancore WHERE nosso_numero = %s",
            (alteracao['numero_boleto'],)
        )
        registro_existente = cursor.fetchone()

        cursor.execute(f"""
            UPDATE {SCHEMA_DB}.boleto_ancore
            SET situacao_boleto = %s
            WHERE nosso_numero = %s
            RETURNING nosso_numero;
        """, (alteracao['situacao_boleto'], alteracao['numero_boleto']))

        registros_atualizados = cursor.fetchall()

        if registros_atualizados and registro_existente:
            situacao_antiga = registro_existente[0]
            if situacao_antiga != alteracao['situacao_boleto']:
                print(
                    f"  [~] ALTERAÇÃO IDENTIFICADA: Boleto {alteracao['numero_boleto']} "
                    f"mudou de '{situacao_antiga}' para '{alteracao['situacao_boleto']}'"
                )
                total_alterados += 1

    return total_alterados

# =============================================================================
# PROCESSAMENTO PAGINADO DE BOLETOS
# =============================================================================

def processar_paginas_de_boletos(
    descricao: str,
    payload: dict,
    url_endpoint: str,
    cursor,
    conn
) -> None:
    """
    Itera pelas páginas da API e realiza upsert dos boletos retornados.

    A paginação é controlada pelo campo `inicio_paginacao` no payload,
    que é incrementado a cada iteração até esgotar todas as páginas.

    Args:
        descricao:    Texto descritivo da busca (aparece nos logs).
        payload:      Dicionário com os filtros da requisição.
        url_endpoint: URL completa do endpoint da API.
        cursor:       Cursor psycopg2 com transação ativa.
        conn:         Conexão psycopg2 para commit após cada página.
    """
    print(f"\n[{NOME_EMPRESA}] Iniciando: {descricao}")
    pagina_atual = 0
    total_novos  = 0

    while True:
        payload["inicio_paginacao"] = pagina_atual

        try:
            resposta = requisitar_com_retry(url_endpoint, payload, f"{descricao} pág {pagina_atual + 1}")

            if resposta is None:
                print(f"[{NOME_EMPRESA}] Falha permanente na página {pagina_atual + 1}. Encerrando {descricao}.")
                break

            if resposta.status_code != 200:
                try:
                    corpo_erro = resposta.json()
                except Exception:
                    corpo_erro = resposta.text

                mensagem_erro = str(corpo_erro).lower()
                eh_erro_sem_dados = resposta.status_code == 406 and (
                    "não foram encontradas" in mensagem_erro or
                    "nenhum registro"       in mensagem_erro
                )

                if eh_erro_sem_dados:
                    print(f"  [-] {descricao}: Nenhum dado para processar.")
                else:
                    print(f"[{NOME_EMPRESA}] Erro HTTP {resposta.status_code} em '{descricao}': {corpo_erro}")
                break

            dados_resposta = resposta.json()
            total_paginas  = dados_resposta.get("numero_paginas", 0)
            boletos_brutos = dados_resposta.get("boletos", [])

            if total_paginas == 0:
                print(f"[{NOME_EMPRESA}] {descricao}: Sem dados para coletar.")
                break

            boletos_normalizados = []
            for boleto_api in boletos_brutos:
                data_pagamento_raw = boleto_api.get("data_pagamento", "")
                veiculos_brutos    = boleto_api.get("veiculos", [])

                mes_referente_raw = boleto_api.get("mes_referente", "")
                try:
                    mes_referente = datetime.strptime(mes_referente_raw, "%m/%Y").date() if mes_referente_raw else None
                except ValueError:
                    mes_referente = None

                for veiculo_ativo in veiculos_brutos:
                    chassi = veiculo_ativo.get("chassi")
                    if not chassi:
                        continue

                    boletos_normalizados.append({
                        "nosso_numero":        boleto_api.get("nosso_numero"),
                        "valor_boleto":        boleto_api.get("valor_boleto"),
                        "tipo_boleto":         boleto_api.get("tipo_boleto"),
                        "data_vencimento":     boleto_api.get("data_vencimento"),
                        "data_pagamento":      data_pagamento_raw if data_pagamento_raw and data_pagamento_raw != "0000-00-00" else None,
                        "situacao_boleto":     boleto_api.get("situacao_boleto"),
                        "mes_referente":       mes_referente,
                        "codigo_veiculo":      veiculo_ativo.get("codigo_veiculo"),
                        "codigo_tipo_veiculo": veiculo_ativo.get("codigo_tipo_veiculo"),
                        "codigo_cooperativa":  veiculo_ativo.get("codigo_cooperativa"),
                        "codigo_voluntario":   veiculo_ativo.get("codigo_voluntario"),
                        "placa":               veiculo_ativo.get("placa"),
                        "chassi":              chassi,
                        "situacao_veiculo":    veiculo_ativo.get("situacao_veiculo")
                    })

            novos, atualizados = upsert_boletos(cursor, boletos_normalizados)
            conn.commit()
            total_novos += novos

            omitidos = len(boletos_normalizados) - novos - atualizados
            print(
                f"[{NOME_EMPRESA}] {descricao} — "
                f"Página {pagina_atual + 1}/{total_paginas} | "
                f"Novos: {novos} | Atualizados: {atualizados} | Inalterados: {omitidos}"
            )

            pagina_atual += 1
            if pagina_atual >= total_paginas:
                break

            time.sleep(1.0)  # Proteção firewall Ancore (max 1 req/s)

        except Exception as erro:
            print(f"[{NOME_EMPRESA}] Exceção durante '{descricao}': {erro}")
            conn.rollback()
            break

    print(f"[{NOME_EMPRESA}] Concluído: {descricao}. Total inserido: {total_novos}")

# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

def main() -> None:
    """
    Orquestra a extração completa de boletos da Ancore.

    Itera mês a mês por data de emissão, do ANO_INICIO/MES_INICIO até o mês atual.
    Buscar por data_emissao futura não faz sentido — boletos só existem após emitidos.
    """
    print(f"\n{'='*60}")
    print(f"  INICIANDO EXTRAÇÃO DE BOLETOS — {NOME_EMPRESA}")
    print(f"{'='*60}\n")

    meses = gerar_meses(ANO_INICIO, MES_INICIO)
    print(f"Extração por data de emissão: {meses[0][0]} até {meses[-1][1]}")
    print(f"Total de meses a processar: {len(meses)}\n")

    try:
        conn   = criar_conexao_db()
        cursor = conn.cursor()
    except Exception as erro:
        print(f"[ERRO CRÍTICO] Não foi possível conectar ao banco de dados: {erro}")
        sys.exit(1)

    for data_inicio, data_fim in meses:
        payload = {
            "data_emissao_inicial":   data_inicio,
            "data_emissao_final":     data_fim,
            "codigo_tipo_boleto":     '',
            "codigo_situacao_boleto": '',  # Puxa todos os status
            "quantidade_por_pagina":  REGISTROS_POR_PAGINA,
            "inicio_paginacao":       0,
        }
        processar_paginas_de_boletos(
            f"Boletos Emitidos ({data_inicio} a {data_fim})",
            payload,
            URL_BASE_API + "/listar/boleto-associado/periodo",
            cursor,
            conn
        )

    cursor.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"  EXTRAÇÃO {NOME_EMPRESA} FINALIZADA COM SUCESSO")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()