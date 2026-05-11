import os
import sys
import time
from datetime import datetime
import requests
import pandas as pd
import pytz
from consulta_sof import input_with_timeout

# Configurações iniciais
tz_brasilia = pytz.timezone("America/Sao_Paulo")
dt_inicio = datetime.fromtimestamp(time.time(), tz=tz_brasilia)
ano = str(dt_inicio.year)
mes = str(dt_inicio.month).zfill(2)
dia = str(dt_inicio.day).zfill(2)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_EXEC = os.path.join(BASE_PATH, "base_execucao")

TOKEN = os.getenv("API_TOKEN_SF")
if not TOKEN:
    TOKEN = input_with_timeout("API_TOKEN_SF não encontrado. Digite o Token")

if not TOKEN:
    print("ERRO CRÍTICO: Token não fornecido.")
    sys.exit(1)

CNPJ = os.getenv("CNPJ_ADESAMPA")
if not CNPJ:
    CNPJ = input_with_timeout("CNPJ_ADESAMPA não encontrado. Digite o CNPJ")

if not CNPJ:
    print("ERRO: CNPJ_ADESAMPA não configurado.")
    sys.exit(1)

print("TOKEN carregado?", bool(TOKEN))
print("Primeiros 6 chars do token:", TOKEN[:6])


def fazer_requisicao(endpoint, params=None, token=None):
    """Realiza requisição GET com tratamento básico de erros e timeout."""
    BASE_URL = "https://gateway.apilib.prefeitura.sp.gov.br/sf/sof/v4/"
    url = f"{BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print(f"Erro: timeout ao conectar com {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro na conexão com {endpoint}: {e}")
        return None


def normalizar_codigo(valor):
    if pd.isna(valor):
        return ""
    texto = str(valor).strip()
    if texto.replace(".", "").isdigit():
        try:
            return str(int(float(texto)))
        except ValueError:
            return texto
    return texto


try:
    df_empenhos = pd.read_excel(os.path.join(BASE_EXEC, "empenhos.xlsx"))
    cod_empenhos = (
        df_empenhos["codEmpenho"]
        .dropna()
        .apply(normalizar_codigo)
        .tolist()
    )
    num_processos = (
        df_empenhos["codProcesso"]
        .dropna()
        .astype(str)
        .tolist()
    )
except Exception as e:
    print(f"Erro ao ler empenhos.xlsx: {e}")
    sys.exit(1)

params_liquidacoes = {
    "codEmpenho": "",
    "anoEmpenho": ano,
    "codEmpresa": "01",
}

list_liquidacoes = []

print(f"Buscando liquidações para {len(cod_empenhos)} empenhos...")
for cod_empenho in cod_empenhos:
    params = params_liquidacoes.copy()
    params["codEmpenho"] = cod_empenho
    resposta = fazer_requisicao("liquidacoes", params=params, token=TOKEN)

    if resposta is None or "lstLiquidacoes" not in resposta or not resposta["lstLiquidacoes"]:
        print(f"⚠️ Resposta inválida ou sem liquidações para codEmpenho {cod_empenho}: {resposta}")
        continue

    df_liquidacoes_temp = pd.json_normalize(resposta["lstLiquidacoes"])
    if not df_liquidacoes_temp.empty:
        print(f"✅ {len(df_liquidacoes_temp)} liquidações para empenho {cod_empenho}")
        list_liquidacoes.append(df_liquidacoes_temp)
    else:
        print(f"⚠️ Nenhuma liquidação retornada para empenho {cod_empenho}")

if list_liquidacoes:
    df_liquidacoes = pd.concat(list_liquidacoes, ignore_index=True)
else:
    df_liquidacoes = pd.DataFrame()

df_liquidacoes.to_excel(os.path.join(BASE_EXEC, "liquidacoes.xlsx"), index=False)
print("✅ Dados de liquidações salvos em liquidacoes.xlsx")

if not df_liquidacoes.empty and "codLiquidacao" in df_liquidacoes.columns:
    cod_liquidacoes = (
        df_liquidacoes["codLiquidacao"]
        .dropna()
        .apply(normalizar_codigo)
        .tolist()
    )
else:
    cod_liquidacoes = []

params_compromissos = {
    "anoEmpenho": ano,
    "cnpjCpfCredor": CNPJ,
    "nrNLP": "",
    "numeroEmpenho": "",
    "nrProcesso": "",
}

list_compromissos = []

# Busca Geral por CNPJ
params = params_compromissos.copy()
params["cnpjCpfCredor"] = CNPJ
resposta = fazer_requisicao("CompromissosPagar", params=params, token=TOKEN)

if resposta is None or "lstCompromisso" not in resposta:
    print(f"⚠️ Resposta inválida para CNPJ {CNPJ}: {resposta}")
    sys.exit(1)

df_compromissos_temp = pd.json_normalize(resposta["lstCompromisso"]) if resposta["lstCompromisso"] else pd.DataFrame()
print(f"Compromissos encontrados para CNPJ {CNPJ}: {len(df_compromissos_temp)}")
if not df_compromissos_temp.empty:
    list_compromissos.append(df_compromissos_temp)

# Busca por NLP / codLiquidacao
if cod_liquidacoes:
    print("Buscando compromissos por NLP...")
    for cod_liquidacao in cod_liquidacoes:
        params = params_compromissos.copy()
        params["nrNLP"] = cod_liquidacao
        resposta = fazer_requisicao("CompromissosPagar", params=params, token=TOKEN)

        if resposta is None or "lstCompromisso" not in resposta or not resposta["lstCompromisso"]:
            print(f"⚠️ Resposta inválida ou sem compromisso para codLiquidacao {cod_liquidacao}: {resposta}")
            continue

        df_temp = pd.json_normalize(resposta["lstCompromisso"])
        print(f"Compromissos encontrados para codLiquidacao {cod_liquidacao}: {len(df_temp)}")
        if not df_temp.empty:
            list_compromissos.append(df_temp)
else:
    print("Pulando busca por NLP (nenhuma liquidação disponível)")

# Busca por empenho
for cod_empenho in cod_empenhos:
    params = params_compromissos.copy()
    params["numeroEmpenho"] = cod_empenho
    resposta = fazer_requisicao("CompromissosPagar", params=params, token=TOKEN)

    if resposta is None or "lstCompromisso" not in resposta or not resposta["lstCompromisso"]:
        print(f"⚠️ Resposta inválida ou sem compromisso para codEmpenho {cod_empenho}: {resposta}")
        continue

    df_temp = pd.json_normalize(resposta["lstCompromisso"])
    print(f"Compromissos encontrados para codEmpenho {cod_empenho}: {len(df_temp)}")
    if not df_temp.empty:
        list_compromissos.append(df_temp)

# Busca por processo
for num_processo in num_processos:
    num_processo_limpo = num_processo.replace(".", "").replace("/", "").replace("-", "")
    params = params_compromissos.copy()
    params["nrProcesso"] = num_processo_limpo
    resposta = fazer_requisicao("CompromissosPagar", params=params, token=TOKEN)

    if resposta is None or "lstCompromisso" not in resposta or not resposta["lstCompromisso"]:
        print(f"⚠️ Resposta inválida ou sem compromisso para numProcesso {num_processo}: {resposta}")
        continue

    df_temp = pd.json_normalize(resposta["lstCompromisso"])
    print(f"Compromissos encontrados para numProcesso {num_processo}: {len(df_temp)}")
    if not df_temp.empty:
        list_compromissos.append(df_temp)

if list_compromissos:
    df_compromissos = pd.concat(list_compromissos, ignore_index=True)
else:
    df_compromissos = pd.DataFrame()

df_compromissos.drop_duplicates(inplace=True)
df_compromissos.to_excel(os.path.join(BASE_EXEC, "compromissos2.xlsx"), index=False)

if not df_compromissos.empty:
    print(f"✅ Total de {len(df_compromissos)} compromissos salvos em compromissos2.xlsx")
else:
    print("⚠️ Nenhum compromisso encontrado.")