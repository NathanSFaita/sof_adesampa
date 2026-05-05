import requests
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta, timezone
import threading
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

def input_with_timeout(prompt, timeout=15):
    """Solicita input do usuário com um tempo limite."""
    print(f"{prompt} (aguardando {timeout}s): ", end="", flush=True)
    result = [None]
    def get_input():
        try:
            result[0] = sys.stdin.readline().rstrip()
        except EOFError:
            pass

    thread = threading.Thread(target=get_input)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        print("\n[TIMEOUT] - Seguindo com variáveis de ambiente...")
        return None
    return result[0]

def main():

    TOKEN = os.getenv("API_TOKEN_SF")
    if not TOKEN:
        TOKEN = input_with_timeout("API_TOKEN_SF não encontrado. Digite o Token")

    if not TOKEN:
        print("ERRO CRÍTICO: Token não fornecido.")
        sys.exit(1)

    print("TOKEN carregado?", bool(TOKEN))
    print("Primeiros 6 chars do token:", TOKEN[:6])

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
            # Tentar converter para numérico
            coerced = pd.to_numeric(df_norm[col], errors='coerce')
            # Se a coluna tem pelo menos um valor numérico, trata como numérica
            if pd.api.types.is_numeric_dtype(coerced) and not coerced.isnull().all():
                # Preencher NaN com 0 para evitar diferenças de nulos
                df_norm[col] = coerced.fillna(0).round(2)
            else:
                df_norm[col] = df_norm[col].astype(str).replace(['nan', 'None', '<NA>'], '').str.strip()
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

    try:
        df_dotacoes = pd.read_excel(os.path.join(BASE_PATH, "arquivos_auxiliares", "dotacoes.xlsx"))
    except FileNotFoundError:
        print("Erro: Arquivo dotacoes.xlsx não encontrado.")
        sys.exit(1)

    list_df_despesas = []

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
            continue
        else:
            print(f"✅ Dotação {dotacao} - Despesas encontradas: {len(resposta['lstDespesas'])}")

            df_despesas = pd.json_normalize(resposta["lstDespesas"])
            # Excluir colunas desnecessárias da API
            df_despesas = df_despesas.drop(columns=['modifiedMode', 'usuarioOperacao'], errors='ignore')
            df_despesas["contrato_gestao"] = contrato_gestao
            df_despesas["secretaria"] = secretaria
            df_despesas["dotacao"] = dotacao
            df_despesas["dotacao_exclusiva"] = dotacao_exclusiva
            list_df_despesas.append(df_despesas)
        
    if list_df_despesas:
        df_final = pd.concat(list_df_despesas, ignore_index=True)
    else:
        df_final = pd.DataFrame(columns=colunas_iniciais)

    df_final['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
    df_final.to_excel(os.path.join(BASE_EXEC, f"execucao.xlsx"), index=False)

    # Consulta à base de empenhos

    # 1. Leia a base antiga ANTES de salvar o novo arquivo
    try:
        empenhos_anterior = pd.read_excel(os.path.join(BASE_PATH, "base_execucao", "empenhos.xlsx"))
    except FileNotFoundError:
        empenhos_anterior = pd.DataFrame()

    CNPJ = os.getenv("CNPJ_ADESAMPA")
    if not CNPJ:
        CNPJ = input_with_timeout("CNPJ_ADESAMPA não encontrado. Digite o CNPJ")

    if not CNPJ:
        print("ERRO: CNPJ_ADESAMPA não configurado.")
        sys.exit(1)

    params_empenhos = {
        "anoEmpenho": ano,
        "mesEmpenho": mes,
        "numCpfCnpj": CNPJ,
        "numPagina": 1
    }

    def col_str(df, col):   
        series = df[col].astype("string").fillna("")
        if col == "codOrgao":
            return series.str.zfill(2)
        return series

    num_pagina = fazer_requisicao("empenhos", params=params_empenhos, token=TOKEN)
    if num_pagina and "metaDados" in num_pagina:
        df_paginas = pd.json_normalize(num_pagina["metaDados"])
        total_paginas = int(df_paginas["qtdPaginas"][0])
    else:
        print("Não foi possível obter dados de empenhos.")
        total_paginas = 0

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

    list_empenhos = []
    df_final_empenhos = pd.DataFrame()

    for pagina in range(1, total_paginas + 1):
        params_empenhos["numPagina"] = pagina
        resposta_empenhos = fazer_requisicao("empenhos", params=params_empenhos, token=TOKEN)
        if not resposta_empenhos or "lstEmpenhos" not in resposta_empenhos:
            continue
        
        df_empenhos = pd.json_normalize(resposta_empenhos["lstEmpenhos"])
        if df_empenhos.empty:
            continue

        df_empenhos["dotacao_completa"] = (
            col_str(df_empenhos, "codOrgao")+ "." +
            col_str(df_empenhos, "codUnidade")+ "." +
            col_str(df_empenhos, "codFuncao")+ "." +
            col_str(df_empenhos, "codSubFuncao")+ "." +
            col_str(df_empenhos, "codPrograma")+ "." +
            col_str(df_empenhos, "codProjetoAtividade")+ "." +
            col_str(df_empenhos, "codCategoria") +
            col_str(df_empenhos, "codGrupo") +
            col_str(df_empenhos, "codModalidade") +
            col_str(df_empenhos, "codElemento") + "00." +
            col_str(df_empenhos, "codFonteRecurso")
        )

        codproc = (
        df_empenhos["codProcesso"]
        .astype("string")
        .fillna("")
        .str.replace(r"\D", "", regex=True)
    )
        codproc = codproc.str.zfill(16)
        has_codproc = codproc.str.len() == 16
        df_empenhos.loc[has_codproc, "codProcesso"] = (
            codproc.str.slice(0, 4)
            + "."
            + codproc.str.slice(4, 8)
            + "/"
            + codproc.str.slice(8, 15)
            + "-"
            + codproc.str.slice(15)
        )

        if "anexos" in df_empenhos.columns:
            anexos_extraidos = df_empenhos["anexos"].apply(extrai_anexo).apply(pd.Series).add_prefix("anexo_")
            df_empenhos = pd.concat([df_empenhos, anexos_extraidos], axis=1).drop(columns=["anexos"], errors='ignore')

        list_empenhos.append(df_empenhos)

    if list_empenhos:
        df_final_empenhos = pd.concat(list_empenhos, ignore_index=True)

    df_final_empenhos['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
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
            if col == "data_hora_extracao":
                continue
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
        df_mudancas['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
        df_mudancas.to_excel(os.path.join(BASE_EXEC, f"mudancas_execucao.xlsx"), index=False)
        print(f"\n📊 Relatório salvo em: mudancas_execucao.xlsx")
    else:
        print("\n✨ Nenhuma mudança detectada!")

    print("="*60)

    # Comparação entre empenhos_anterior e df_final_empenhos
    print("\n" + "="*60)
    print("ANÁLISE DE MUDANÇAS - EMPENHOS")
    print("="*60)

    # DEBUG: Verificar se os dataframes têm dados
    print(f"DEBUG: empenhos_anterior shape: {empenhos_anterior.shape}")
    print(f"DEBUG: df_final_empenhos shape: {df_final_empenhos.shape}")
    print(f"DEBUG: empenhos_anterior colunas: {list(empenhos_anterior.columns)}")
    print(f"DEBUG: df_final_empenhos colunas: {list(df_final_empenhos.columns)}")

    # Normalizar dataframes
    empenhos_anterior_norm = normalizar_para_comparacao(empenhos_anterior)
    df_final_empenhos_norm = normalizar_para_comparacao(df_final_empenhos)

    print(f"DEBUG: empenhos_anterior_norm shape: {empenhos_anterior_norm.shape}")
    print(f"DEBUG: df_final_empenhos_norm shape: {df_final_empenhos_norm.shape}")

    # Resetar índices para comparação
    empenhos_anterior_reset = empenhos_anterior_norm.reset_index(drop=True)
    df_final_empenhos_reset = df_final_empenhos_norm.reset_index(drop=True)

    # Encontrar colunas comuns
    colunas_comuns_emp = list(set(empenhos_anterior_reset.columns) & set(df_final_empenhos_reset.columns))
    print(f"DEBUG: Colunas comuns: {len(colunas_comuns_emp)}")

    # Helper para garantir que o ID seja uma string limpa
    def format_id_key(val):
        s = str(val).strip()
        if s.endswith('.0'):
            return s[:-2]
        return s

    # Verificar se codEmpenho existe
    if 'codEmpenho' in empenhos_anterior_reset.columns:
        anterior_emp_dict = {format_id_key(row['codEmpenho']): row.to_dict() for idx, row in empenhos_anterior_reset.iterrows()}
    else:
        print("DEBUG: 'codEmpenho' NÃO encontrado em empenhos_anterior_reset")
        anterior_emp_dict = {}

    if 'codEmpenho' in df_final_empenhos_reset.columns:
        final_emp_dict = {format_id_key(row['codEmpenho']): row.to_dict() for idx, row in df_final_empenhos_reset.iterrows()}
    else:
        print("DEBUG: 'codEmpenho' NÃO encontrado em df_final_empenhos_reset")
        final_emp_dict = {}

    print(f"DEBUG: anterior_emp_dict tamanho: {len(anterior_emp_dict)}")
    print(f"DEBUG: final_emp_dict tamanho: {len(final_emp_dict)}")
    print(f"DEBUG: Primeiras 3 chaves anterior: {list(anterior_emp_dict.keys())[:3]}")
    print(f"DEBUG: Primeiras 3 chaves final: {list(final_emp_dict.keys())[:3]}")

    # Criar dataframe para registrar mudanças
    mudancas_emp = []

    # 1. Linhas removidas
    ids_removidos = set(anterior_emp_dict.keys()) - set(final_emp_dict.keys())
    print(f"DEBUG: IDs removidos: {len(ids_removidos)}")

    if ids_removidos:
        print(f"\n❌ {len(ids_removidos)} empenhos REMOVIDOS")
        for eid in ids_removidos:
            row = anterior_emp_dict[eid]
            mudancas_emp.append({
                "tipo_mudanca": "REMOVIDA",
                "dotacao": row.get('dotacao_completa', ''),
                "codEmpenho": eid,
                "numProcesso": row.get('codProcesso', ''),
                "numeroOriginalContrato": row.get('numeroOriginalContrato', ''),
                "detalhes": "Removido da base atual"
            })

    # 2. Linhas adicionadas
    ids_adicionados = set(final_emp_dict.keys()) - set(anterior_emp_dict.keys())
    print(f"DEBUG: IDs adicionados: {len(ids_adicionados)}")

    if ids_adicionados:
        print(f"\n✅ {len(ids_adicionados)} empenhos ADICIONADOS")
        for eid in ids_adicionados:
            row = final_emp_dict[eid]
            mudancas_emp.append({
                "tipo_mudanca": "ADICIONADA",
                "dotacao": row.get('dotacao_completa', ''),
                "codEmpenho": eid,
                "numProcesso": row.get('codProcesso', ''),
                "numeroOriginalContrato": row.get('numeroOriginalContrato', '')
            })

    # 3. Linhas modificadas
    ids_comuns = set(anterior_emp_dict.keys()) & set(final_emp_dict.keys())
    print(f"DEBUG: IDs comuns: {len(ids_comuns)}")

    for eid in ids_comuns:
        linha_ant = anterior_emp_dict[eid]
        linha_fin = final_emp_dict[eid]
        
        for col in colunas_comuns_emp:
            if col in ['codEmpenho', 'dotacao_completa', 'data_hora_extracao']:
                continue
                
            val_ant = linha_ant.get(col)
            val_fin = linha_fin.get(col)
            
            if pd.isna(val_ant) != pd.isna(val_fin) or (not pd.isna(val_ant) and val_ant != val_fin):
                mudancas_emp.append({
                    "tipo_mudanca": "MODIFICADA",
                    "dotacao": linha_fin.get('dotacao_completa', ''),
                    "codEmpenho": eid,
                    "numProcesso": linha_fin.get('codProcesso', ''),
                    "numeroOriginalContrato": linha_fin.get('numeroOriginalContrato', ''),
                    "coluna": col,
                    "valor_anterior": str(val_ant),
                    "valor_novo": str(val_fin)
                })

    # Salvar relatório
    if mudancas_emp:
        df_mudancas_emp = pd.DataFrame(mudancas_emp)
        df_mudancas_emp['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
        df_mudancas_emp.to_excel(os.path.join(BASE_EXEC, f"mudancas_empenhos.xlsx"), index=False)
        print(f"\n📊 Relatório salvo em: mudancas_empenhos.xlsx")
    else:
        print(f"\nDEBUG: mudancas_emp está vazio - total de mudanças registradas: {len(mudancas_emp)}")
        print("\n✨ Nenhuma mudança detectada nos empenhos!")

    print("="*60)

if __name__ == "__main__":
    main()