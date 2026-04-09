# ARX Extração — Ancore

Pipeline de extração, transformação e carga (ETL) de dados da API Hinova SGA para um banco de dados PostgreSQL.

---

## Visão Geral

Este projeto coleta dados de veículos, boletos, cancelamentos, retenções e pendências da associação **Ancore** via API REST da Hinova, e os persiste em um banco PostgreSQL para análise e reporting.

---

## Scripts

| Script | Descrição |
|---|---|
| `ancore_veiculo.py` | Carga completa de veículos por situação (todos os códigos 1–24) |
| `ancore_veiculo_cadastro.py` | Carga incremental diária de veículos por data de cadastro |
| `ancore_atualiza_veiculo.py` | Sincronização de alterações de veículos (últimos 7 dias) |
| `ancore_boleto.py` | Carga histórica de boletos por mês |
| `ancore_atualiza_boleto.py` | Atualização diária de boletos alterados |
| `ancore_cancelamento.py` | Histórico de cancelamentos via alterações de situação |
| `ancore_retencao.py` | Atendimentos de retenção por tipo e data |
| `ancore_pendencias.py` | Leitura de pendências via Google Sheets e carga no banco |
| `dados.py` | Módulo central de configuração (headers, datas, credenciais via `.env`) |
| `automa.py` | Orquestrador de execução dos scripts |

---

## Estrutura do Banco

Todas as tabelas ficam no schema `ancore` no PostgreSQL.

| Tabela | Script de origem |
|---|---|
| `ancore.veiculo` | `ancore_veiculo.py`, `ancore_veiculo_cadastro.py` |
| `ancore.cancelamentos` | `ancore_cancelamento.py` |
| `ancore.retencao` | `ancore_retencao.py` |
| `ancore.pendencias` | `ancore_pendencias.py` |

---

## Configuração

### 1. Variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
URL_SGA=https://api.hinova.com.br/api/sga/v2

DBNAME=postgres
DBUSER=seu_usuario
DBPASS=sua_senha
DBHOST=seu_host
DBPORT=5434

HEADERS_ANCORE={"Content-Type": "application/json", "Authorization": "Bearer SEU_TOKEN"}
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Google Sheets (apenas `ancore_pendencias.py`)

- Crie uma Service Account no Google Cloud Console
- Ative a **Google Sheets API** no projeto
- Baixe o JSON de credenciais e salve como `credenciais.json` na raiz
- Compartilhe a planilha com o e-mail da Service Account

> `credenciais.json` e `.env` estão no `.gitignore` e nunca devem ser commitados.

---

## Execução

Cada script pode ser executado individualmente:

```bash
python ancore_cancelamento.py
python ancore_retencao.py
python ancore_veiculo.py
```

Ou via orquestrador:

```bash
python automa.py
```

---

## Dependências

```
requests
psycopg2
python-dotenv
google-auth
google-auth-httplib2
google-api-python-client
playwright
```
