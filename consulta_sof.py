import requests
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta, timezone
import pytz

# Configurações iniciais

tz_brasilia = pytz.timezone('America/Sao_Paulo')
dt_inicio = datetime.fromtimestamp(time.time(), tz=tz_brasilia)
ano = str(dt_inicio.year)
mes = str(dt_inicio.month).zfill(2)
dia = str(dt_inicio.day).zfill(2)

print(dt_inicio)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_EXEC = os.path.join(BASE_PATH, "base_execucao")


TOKEN = os.getenv("API_TOKEN_SF")
if not TOKEN:
    TOKEN = input("Digite o token de acesso à API: ").strip()

print("TOKEN carregado?", bool(TOKEN))
print("Primeiros 6 chars do token:", TOKEN[:6] if TOKEN else "NULO")

def fazer_requisicao(endpoint, params=None, token=None):
    BASE_URL = "https://gateway.apilib.prefeitura.sp.gov.br/sf/sof/v4/"
    url = f"{BASE_URL}{endpoint}"
    headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()

        print(f"Requisição para {endpoint} com params {params} retornou status {response.status_code}")     
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None
    
def normalizar_para_comparacao(df):
    """Normaliza dataframe para comparação consistente de valores"""
    df_norm = df.copy()
    
    for col in df_norm.columns:
        # Converter para string
        df_norm[col] = df_norm[col].astype(str)
        
        # Substituir 'nan' e 'None' por vazio
        df_norm[col] = df_norm[col].replace(['nan', 'None', '<NA>'], '')
        
        # Remover espaços extras
        df_norm[col] = df_norm[col].str.strip()
        
        # Padronizar valores vazios
        df_norm[col] = df_norm[col].replace('', None)
    
    return df_norm
    
# Consulta à base de despesas

despesas_anterior = pd.read_excel(os.path.join(BASE_PATH, "base_execucao", "execucao.xlsx"))

params_dp = {
    "anoDotacao": ano,
    "mesDotacao": mes,
    "codOrgao": "",
    "codUnidade": "",
    "codFuncao": "",
    "codSubFuncao": "",
    "codPrograma": "",
    "codProjetoAtividade": "",
    "codCategoria": "",
    "codGrupo": "", 
    "codModalidade": "",
    "codElemento": "",
    "codFonteRecurso": "",
    "codVinculacaoRecurso": "",  
    }    
    
colunas_iniciais = ["contrato_gestao", "secretaria", "dotacao", "dotacao_exclusiva"]
df_final = pd.DataFrame(columns=colunas_iniciais)

df_dotacoes = pd.read_excel(os.path.join(BASE_PATH, "arquivos_auxiliares", "dotacoes.xlsx"))

for index, row in df_dotacoes.iterrows():
    contrato_gestao = str(row["contrato_gestao"])
    secretaria = str(row["secretaria"])
    orgao = str(row["orgao"])
    uo = str(row["uo"])
    funcao = str(row["funcao"])
    subfuncao = str(row["subfuncao"])
    programa = str(row["programa"])
    proj_ativ = str(row["proj_ativ"])
    despesa = str(row["despesa"])
    fonte = str(row["fonte"]).zfill(2)
    referencia = str(row["referencia"])
    destinacao = str(row["destinacao"]).zfill(3)
    vinculacao = str(row["vinculacao"]).zfill(4)
    dotacao_exclusiva = bool(int(row["dotacao_exclusiva"]))

    dotacao = f"{orgao}.{uo}.{funcao}.{subfuncao}.{programa}.{proj_ativ}.{despesa}.{fonte}.{referencia}.{destinacao}.{vinculacao}"

    params_dp["codOrgao"] = orgao
    params_dp["codUnidade"] = uo
    params_dp["codFuncao"] = funcao
    params_dp["codSubFuncao"] = subfuncao
    params_dp["codPrograma"] = programa
    params_dp["codProjetoAtividade"] = proj_ativ
    params_dp["codCategoria"] = str(row["categoria"])
    params_dp["codGrupo"] = str(row["grupo"])
    params_dp["codModalidade"] = str(row["modalidade"])
    params_dp["codElemento"] = str(row["elemento"])
    params_dp["codFonteRecurso"] = fonte
    params_dp["codVinculacaoRecurso"] = vinculacao

    resposta = fazer_requisicao("despesas", params=params_dp, token=TOKEN)
    if resposta is None or "lstDespesas" not in resposta:
        print(f"⚠️ Resposta inválida para dotação {dotacao}")
    else:
        print(f"✅ Dotação {dotacao} - Despesas encontradas: {len(resposta['lstDespesas'])}")

    df_despesas = pd.json_normalize(resposta["lstDespesas"])

    # Excluir colunas desnecessárias da API
    df_despesas = df_despesas.drop(columns=['modifiedMode', 'usuarioOperacao'], errors='ignore')

    df_despesas["contrato_gestao"] = contrato_gestao
    df_despesas["secretaria"] = secretaria
    df_despesas["dotacao"] = dotacao
    df_despesas["dotacao_exclusiva"] = dotacao_exclusiva

    df_final = pd.concat([df_final, df_despesas], ignore_index=True)
    
df_final.to_excel(os.path.join(BASE_EXEC, f"execucao.xlsx"), index=False)

# Consulta à base de empenhos

CNPJ = os.getenv("CNPJ_ADESAMPA")
if not CNPJ:
    CNPJ = input("Digite o CNPJ da empresa: ").strip()

params_empenhos = {
    "anoEmpenho": ano,
    "mesEmpenho": mes,
    "numCpfCnpj": CNPJ,
    "numPagina": ""
}

df_final_empenhos = pd.DataFrame()
num_pagina = fazer_requisicao("empenhos", params=params_empenhos, token=TOKEN)
df_paginas = pd.json_normalize(num_pagina["metaDados"])
total_paginas = df_paginas["qtdPaginas"][0]

pagina = 0
for p in range(total_paginas):
    pagina += 1
    params_empenhos["numPagina"] = pagina
    resposta_empenhos = fazer_requisicao("empenhos", params=params_empenhos, token=TOKEN)
    
    df_empenhos = pd.json_normalize(resposta_empenhos["lstEmpenhos"])

    def col_str(col):   
        series = df_empenhos[col].astype("string").fillna("")
        if col == "codOrgao":
            return series.str.zfill(2) # Garante que o código do órgão tenha sempre 2 dígitos
        return series

    df_empenhos["dotacao_completa"] = (
        col_str("codOrgao")+ "." +
        col_str("codUnidade")+ "." +
        col_str("codFuncao")+ "." +
        col_str("codSubFuncao")+ "." +
        col_str("codPrograma")+ "." +
        col_str("codProjetoAtividade")+ "." +
        col_str("codCategoria")+
        col_str("codGrupo")+
        col_str("codModalidade")+
        col_str("codElemento")+"00" + "." +
        col_str("codFonteRecurso")
    )

def extrai_anexo(anexos):
    if isinstance(anexos, list) and len(anexos) > 0:
        dados_concatenados = {}
        for item in anexos:
            if isinstance(item, dict):
                for k, v in item.items():
                    if v is not None:
                        val_str = str(v)
                        if k in dados_concatenados:
                            dados_concatenados[k] += " | " + val_str
                        else:
                            dados_concatenados[k] = val_str
        return dados_concatenados
    return {}

if "anexos" in df_empenhos.columns:
    anexos_extraidos = df_empenhos["anexos"].apply(extrai_anexo).apply(pd.Series).add_prefix("anexo_")
    df_empenhos = pd.concat([df_empenhos, anexos_extraidos], axis=1).drop(columns=["anexos"], errors='ignore')

df_final_empenhos = pd.concat([df_final_empenhos, df_empenhos], ignore_index=True)

df_final_empenhos.to_excel(os.path.join(BASE_EXEC, f"empenhos.xlsx"), index=False)

# Comparação entre despesas_anterior e df_final
print("\n" + "="*60)
print("ANÁLISE DE MUDANÇAS")
print("="*60)

# Normalizar dataframes
despesas_anterior_norm = normalizar_para_comparacao(despesas_anterior)
df_final_norm = normalizar_para_comparacao(df_final)

# Resetar índices para comparação
despesas_anterior_reset = despesas_anterior_norm.reset_index(drop=True)
df_final_reset = df_final_norm.reset_index(drop=True)

# Criar dataframe para registrar mudanças
mudancas_exec = []

# Encontrar colunas comuns
colunas_comuns = list(set(despesas_anterior_reset.columns) & set(df_final_reset.columns))

# Criar dicionários indexados por dotacao para comparação mais precisa
anterior_por_dotacao = {str(row.get('dotacao', '')): row for idx, row in despesas_anterior_reset.iterrows()}
final_por_dotacao = {str(row.get('dotacao', '')): row for idx, row in df_final_reset.iterrows()}

# 1. Linhas removidas (em anterior mas não em final)
dotacoes_removidas = set(anterior_por_dotacao.keys()) - set(final_por_dotacao.keys())
if len(dotacoes_removidas) > 0:
    print(f"\n❌ {len(dotacoes_removidas)} linhas REMOVIDAS")
    for dotacao in dotacoes_removidas:
        row = anterior_por_dotacao[dotacao]
        mudancas_exec.append({
            "tipo_mudanca": "REMOVIDA",
            "dotacao": dotacao,
            "dotacao_exclusiva": row.get('dotacao_exclusiva', ''),
            "detalhes": str(row.to_dict())
        })

# 2. Linhas adicionadas (em final mas não em anterior)
dotacoes_adicionadas = set(final_por_dotacao.keys()) - set(anterior_por_dotacao.keys())
if len(dotacoes_adicionadas) > 0:
    print(f"\n✅ {len(dotacoes_adicionadas)} linhas ADICIONADAS")
    for dotacao in dotacoes_adicionadas:
        row = final_por_dotacao[dotacao]
        mudancas_exec.append({
            "tipo_mudanca": "ADICIONADA",
            "dotacao": dotacao,
            "dotacao_exclusiva": row.get('dotacao_exclusiva', '')
        })

# 3. Linhas modificadas (comparar célula por célula)
dotacoes_comuns = set(anterior_por_dotacao.keys()) & set(final_por_dotacao.keys())
linhas_modificadas = []

for dotacao in dotacoes_comuns:
    linha_anterior = anterior_por_dotacao[dotacao]
    linha_final = final_por_dotacao[dotacao]
    
    for col in colunas_comuns:
        if linha_anterior[col] != linha_final[col]:
            linhas_modificadas.append({
                "dotacao": dotacao,
                "dotacao_exclusiva": linha_final.get('dotacao_exclusiva', ''),
                "coluna": col,
                "valor_anterior": linha_anterior[col],
                "valor_novo": linha_final[col]
            })

if len(linhas_modificadas) > 0:
    print(f"\n🔄 {len(linhas_modificadas)} MUDANÇAS DE VALORES")
    for mudanca in linhas_modificadas:
        print(f"   Dotação {mudanca['dotacao']}, Coluna '{mudanca['coluna']}':")
        print(f"   {mudanca['valor_anterior']} → {mudanca['valor_novo']}")
        mudancas_exec.append({
            "tipo_mudanca": "MODIFICADA",
            "dotacao": mudanca['dotacao'],
            "dotacao_exclusiva": mudanca['dotacao_exclusiva'],
            "coluna": mudanca['coluna'],
            "valor_anterior": mudanca['valor_anterior'],
            "valor_novo": mudanca['valor_novo']
        })

# Salvar relatório de mudanças
if mudancas_exec:
    df_mudancas = pd.DataFrame(mudancas_exec)
    df_mudancas.to_excel(os.path.join(BASE_EXEC, f"mudancas_{dia}_{mes}_{ano}.xlsx"), index=False)
    print(f"\n📊 Relatório salvo em: mudancas_{dia}_{mes}_{ano}.xlsx")
else:
    print("\n✨ Nenhuma mudança detectada!")

print("="*60)

# Comparação entre empenhos_anterior e df_final_empenhos
print("\n" + "="*60)
print("ANÁLISE DE MUDANÇAS - EMPENHOS")
print("="*60)

empenhos_anterior = pd.read_excel(os.path.join(BASE_PATH, "base_execucao", "empenhos.xlsx"))

# Normalizar dataframes
empenhos_anterior_norm = normalizar_para_comparacao(empenhos_anterior)
df_final_empenhos_norm = normalizar_para_comparacao(df_final_empenhos)

# Resetar índices para comparação
empenhos_anterior_reset = empenhos_anterior_norm.reset_index(drop=True)
df_final_empenhos_reset = df_final_empenhos_norm.reset_index(drop=True)

# Criar dataframe para registrar mudanças
mudancas_emp = []

# Encontrar colunas comuns
colunas_comuns_emp = list(set(empenhos_anterior_reset.columns) & set(df_final_empenhos_reset.columns))

# Criar dicionários indexados por dotacao_completa para comparação mais precisa
anterior_empenhos_por_dotacao = {str(row.get('dotacao_completa', '')): row for idx, row in empenhos_anterior_reset.iterrows()}
final_empenhos_por_dotacao = {str(row.get('dotacao_completa', '')): row for idx, row in df_final_empenhos_reset.iterrows()}

# 1. Linhas removidas (em anterior mas não em final)
dotacoes_emp_removidas = set(anterior_empenhos_por_dotacao.keys()) - set(final_empenhos_por_dotacao.keys())
if len(dotacoes_emp_removidas) > 0:
    print(f"\n❌ {len(dotacoes_emp_removidas)} linhas REMOVIDAS")
    for dotacao in dotacoes_emp_removidas:
        row = anterior_empenhos_por_dotacao[dotacao]
        mudancas_emp.append({
            "tipo_mudanca": "REMOVIDA",
            "dotacao": dotacao,
            "numEmpenho": row.get('numEmpenho', ''),
            "detalhes": str(row.to_dict())
        })

# 2. Linhas adicionadas (em final mas não em anterior)
dotacoes_emp_adicionadas = set(final_empenhos_por_dotacao.keys()) - set(anterior_empenhos_por_dotacao.keys())
if len(dotacoes_emp_adicionadas) > 0:
    print(f"\n✅ {len(dotacoes_emp_adicionadas)} linhas ADICIONADAS")
    for dotacao in dotacoes_emp_adicionadas:
        row = final_empenhos_por_dotacao[dotacao]
        mudancas_emp.append({
            "tipo_mudanca": "ADICIONADA",
            "dotacao": dotacao,
            "numEmpenho": row.get('numEmpenho', '')
        })

# 3. Linhas modificadas (comparar célula por célula apenas nas colunas de valor)
colunas_valores = ["valTotalEmpenhado", "valAnuladoEmpenho", "valEmpenhadoLiquido", 
                   "valLiquidado", "valPagoExercicio", "valPagoRestos"]

dotacoes_emp_comuns = set(anterior_empenhos_por_dotacao.keys()) & set(final_empenhos_por_dotacao.keys())
linhas_emp_modificadas = []

for dotacao in dotacoes_emp_comuns:
    linha_anterior = anterior_empenhos_por_dotacao[dotacao]
    linha_final = final_empenhos_por_dotacao[dotacao]
    
    # Comparar apenas as colunas de valor que existem
    colunas_a_comparar = [col for col in colunas_valores if col in colunas_comuns_emp]
    
    for col in colunas_a_comparar:
        if linha_anterior[col] != linha_final[col]:
            linhas_emp_modificadas.append({
                "dotacao": dotacao,
                "numEmpenho": linha_final.get('numEmpenho', ''),
                "coluna": col,
                "valor_anterior": linha_anterior[col],
                "valor_novo": linha_final[col]
            })

if len(linhas_emp_modificadas) > 0:
    print(f"\n🔄 {len(linhas_emp_modificadas)} MUDANÇAS DE VALORES")
    for mudanca in linhas_emp_modificadas:
        print(f"   Dotação {mudanca['dotacao']}, Coluna '{mudanca['coluna']}':")
        print(f"   {mudanca['valor_anterior']} → {mudanca['valor_novo']}")
        mudancas_emp.append({
            "tipo_mudanca": "MODIFICADA",
            "dotacao": mudanca['dotacao'],
            "numEmpenho": mudanca['numEmpenho'],
            "coluna": mudanca['coluna'],
            "valor_anterior": mudanca['valor_anterior'],
            "valor_novo": mudanca['valor_novo']
        })

# Salvar relatório de mudanças de empenhos
if mudancas_emp:
    df_mudancas_emp = pd.DataFrame(mudancas_emp)
    df_mudancas_emp.to_excel(os.path.join(BASE_EXEC, f"mudancas_empenhos_{dia}_{mes}_{ano}.xlsx"), index=False)
    print(f"\n📊 Relatório salvo em: mudancas_empenhos_{dia}_{mes}_{ano}.xlsx")
else:
    print("\n✨ Nenhuma mudança detectada nos empenhos!")

print("="*60)

