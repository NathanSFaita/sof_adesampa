import requests
import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta, timezone
import threading
import pytz
import io 

# Google Drive API
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload 

# Configurações iniciais
tz_brasilia = pytz.timezone('America/Sao_Paulo')
dt_inicio = datetime.fromtimestamp(time.time(), tz=tz_brasilia)
ano = str(dt_inicio.year)
mes = str(dt_inicio.month).zfill(2)
dia = str(dt_inicio.day).zfill(2)

print(f"Iniciando rotina em: {dt_inicio.strftime('%d/%m/%Y %H:%M:%S')}")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_EXEC = os.path.join(BASE_PATH, "base_execucao")

SCOPES = ["https://www.googleapis.com/auth/drive"]

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

def formatar_brl(valor):
    """Formata um valor numérico para o formato BRL (R$ x.xxx,xx)"""
    if pd.isna(valor) or str(valor).strip() in ['', 'nan', '-', 'None']:
        return valor if valor == '-' else '-'
    try:
        if isinstance(valor, str) and valor.startswith('R$'):
            return valor
        num = float(str(valor).replace('.', '').replace(',', '.')) if isinstance(valor, str) else float(valor)
        return f"R$ {num:,.2f}".replace(',', 'TEMP_COMMA').replace('.', ',').replace('TEMP_COMMA', '.')
    except:
        return str(valor)

# --- Funções Auxiliares do Google Drive ---

def build_drive_service(service_account_file):
    if not os.path.exists(service_account_file):
        print(f"Erro: serviço de conta Google não encontrado em {service_account_file}")
        return None
    try:
        creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"Erro ao criar serviço do Google Drive: {e}")
        return None

def get_file_in_folder(service, file_name, folder_id):
    try:
        response = service.files().list(
            q=f"name = '{file_name}' and '{folder_id}' in parents and trashed = false",
            fields="files(id, name)",
            pageSize=1,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = response.get("files", [])
        return files[0]["id"] if files else None
    except Exception as e:
        print(f"Erro ao buscar arquivo no Drive ({file_name}): {e}")
        return None

# --- NOVA FUNÇÃO DE DOWNLOAD ---
def download_file_from_drive(service, file_name, folder_id, dest_path):
    try:
        file_id = get_file_in_folder(service, file_name, folder_id)
        if not file_id:
            print(f"Aviso: {file_name} não encontrado no Drive. Usando base local vazia/existente.")
            return False
        
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(dest_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        print(f"✅ Download do Drive concluído: {file_name}")
        return True
    except Exception as e:
        print(f"Erro ao baixar {file_name} do Drive: {e}")
        return False

def upload_or_update_file(service, file_path, folder_id):
    if not os.path.exists(file_path):
        print(f"Aviso: arquivo não existe para upload no Drive: {file_path}")
        return None
    file_name = os.path.basename(file_path)
    try:
        existing_file_id = get_file_in_folder(service, file_name, folder_id)
        media = MediaFileUpload(file_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        if existing_file_id:
            try:
                service.files().update(fileId=existing_file_id, media_body=media, supportsAllDrives=True).execute()
                print(f"Arquivo atualizado no Google Drive: {file_name}")
                return existing_file_id
            except Exception as update_error:
                if "404" in str(update_error) or "notFound" in str(update_error):
                    metadata = {"name": file_name, "parents": [folder_id]}
                    created = service.files().create(body=metadata, media_body=media, fields="id", supportsAllDrives=True).execute()
                    print(f"Arquivo criado no Google Drive: {file_name}")
                    return created.get("id")
                else:
                    print(f"Erro ao atualizar arquivo no Google Drive ({file_name}): {update_error}")
                    return None
        else:
            metadata = {"name": file_name, "parents": [folder_id]}
            created = service.files().create(body=metadata, media_body=media, fields="id", supportsAllDrives=True).execute()
            print(f"Arquivo criado no Google Drive: {file_name}")
            return created.get("id")
    except Exception as e:
        print(f"Erro ao enviar arquivo para o Google Drive ({file_name}): {e}")
        return None

def main():
    TOKEN = os.getenv("API_TOKEN_SF")
    if not TOKEN:
        TOKEN = input_with_timeout("API_TOKEN_SF não encontrado. Digite o Token")

    if not TOKEN:
        print("ERRO CRÍTICO: Token não fornecido.")
        sys.exit(1)

    # Configurações do Google Drive
    SERVICE_ACCOUNT_FILE = os.getenv(
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        os.path.join(BASE_PATH, "service_account.json")
    )
    DRIVE_FOLDER_IDS_RAW = os.getenv("DRIVE_FOLDER_ID")
    if not DRIVE_FOLDER_IDS_RAW:
        DRIVE_FOLDER_IDS_RAW = input_with_timeout("Digite os IDs das pastas do Google Drive separados por vírgula: ", timeout=30)

    # Cria uma lista de IDs limpando os espaços em branco
    DRIVE_FOLDER_IDS = [fid.strip() for fid in DRIVE_FOLDER_IDS_RAW.split(',')] if DRIVE_FOLDER_IDS_RAW else []
    # Define a primeira pasta da lista como a pasta principal para os downloads
    PRIMARY_DRIVE_FOLDER_ID = DRIVE_FOLDER_IDS[0] if DRIVE_FOLDER_IDS else None

    # -------------------------------------------------------------
    # NOVO FLUXO: DOWNLOAD DA BASE HISTÓRICA DO DRIVE
    # -------------------------------------------------------------
    os.makedirs(BASE_EXEC, exist_ok=True) # Garante que a pasta base_execucao existe
    drive_service = build_drive_service(SERVICE_ACCOUNT_FILE) if PRIMARY_DRIVE_FOLDER_ID else None
    
    if drive_service and PRIMARY_DRIVE_FOLDER_ID:
        print("\n" + "="*60)
        print(f"BAIXANDO BASES MAIS RECENTES DO GOOGLE DRIVE (Pasta Principal)")
        print("="*60)
        download_file_from_drive(drive_service, "execucao.xlsx", PRIMARY_DRIVE_FOLDER_ID, os.path.join(BASE_EXEC, "execucao.xlsx"))
        download_file_from_drive(drive_service, "empenhos.xlsx", PRIMARY_DRIVE_FOLDER_ID, os.path.join(BASE_EXEC, "empenhos.xlsx"))

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
            print(f"Erro na requisição ({endpoint}): {e}")
            return None
        
    def normalizar_para_comparacao(df):
        """Normaliza dataframe para comparação consistente de valores"""
        if df.empty:
            return df.copy()
        df_norm = df.copy()
        cols_texto = ['dotacao', 'codEmpenho', 'codProcesso', 'dotacao_completa', 'numeroOriginalContrato', 'Processo SEI']
        
        for col in df_norm.columns:
            if col in cols_texto:
                df_norm[col] = df_norm[col].astype(str).replace(['nan', 'None', '<NA>'], '').str.strip()
                continue
                
            coerced = pd.to_numeric(df_norm[col], errors='coerce')
            if pd.api.types.is_numeric_dtype(coerced) and not coerced.isnull().all():
                df_norm[col] = coerced.fillna(0).round(2)
            else:
                df_norm[col] = df_norm[col].astype(str).replace(['nan', 'None', '<NA>'], '').str.strip()
                df_norm[col] = df_norm[col].replace('', None)
        return df_norm

    # -------------------------------------------------------------
    # 1. CONSULTA DE DESPESAS (EXECUÇÃO ORÇAMENTÁRIA)
    # -------------------------------------------------------------
    try:
        despesas_anterior = pd.read_excel(os.path.join(BASE_PATH, "base_execucao", "execucao.xlsx"))
    except FileNotFoundError:
        despesas_anterior = pd.DataFrame()

    params_dp = {
        "anoDotacao": ano, "mesDotacao": mes, "codOrgao": "", "codUnidade": "",
        "codFuncao": "", "codSubFuncao": "", "codPrograma": "", "codProjetoAtividade": "",
        "codCategoria": "", "codGrupo": "", "codModalidade": "", "codElemento": "",
        "codFonteRecurso": "", "codVinculacaoRecurso": "",  
    }    
        
    colunas_iniciais = ["contrato_gestao", "secretaria", "dotacao", "dotacao_exclusiva"]

    try:
        df_dotacoes = pd.read_excel(os.path.join(BASE_PATH, "arquivos_auxiliares", "dotacoes.xlsx"))
    except FileNotFoundError:
        print("Erro: Arquivo dotacoes.xlsx não encontrado.")
        sys.exit(1)

    lookup_orgao_proj_ativ_sigla = {}
    lookup_orgao_sigla = {}
    
    for _, row in df_dotacoes.iterrows():
        orgao = str(row['orgao'])
        proj_ativ = str(row['proj_ativ'])
        sigla = str(row['contrato_gestao'])
        lookup_orgao_proj_ativ_sigla[f"{orgao}_{proj_ativ}"] = sigla
        lookup_orgao_sigla[orgao] = sigla

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

        params_dp.update({
            "codOrgao": orgao, "codUnidade": uo, "codFuncao": funcao, "codSubFuncao": subfuncao,
            "codPrograma": programa, "codProjetoAtividade": proj_ativ, "codCategoria": str(row["categoria"]),
            "codGrupo": str(row["grupo"]), "codModalidade": str(row["modalidade"]),
            "codElemento": str(row["elemento"]), "codFonteRecurso": fonte, "codVinculacaoRecurso": vinculacao
        })

        resposta = fazer_requisicao("despesas", params=params_dp, token=TOKEN)
        if resposta is None or "lstDespesas" not in resposta:
            print(f"⚠️ Resposta inválida/vazia para dotação {dotacao}")
            continue
        else:
            print(f"✅ Dotação {dotacao} - Despesas encontradas: {len(resposta['lstDespesas'])}")
            df_despesas = pd.json_normalize(resposta["lstDespesas"])
            df_despesas = df_despesas.drop(columns=['modifiedMode', 'usuarioOperacao'], errors='ignore')
            df_despesas["contrato_gestao"] = contrato_gestao
            df_despesas["secretaria"] = secretaria
            df_despesas["dotacao"] = dotacao
            df_despesas["dotacao_exclusiva"] = dotacao_exclusiva
            list_df_despesas.append(df_despesas)
        
    df_final = pd.concat(list_df_despesas, ignore_index=True) if list_df_despesas else pd.DataFrame(columns=colunas_iniciais)

    sucesso_execucao = not df_final.empty

    if sucesso_execucao:
        df_final['saldo_dotacao'] = df_final['valDisponivel'] - df_final['valReservadoLiquido']
        df_final['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')

        colunas_execucao = [
            "contrato_gestao", "secretaria", "dotacao", "dotacao_exclusiva", "valOrcadoInicial",
            "valSuplementado", "valReduzido", "valOrcadoAtualizado", "valCongelado", "valDescongelado",
            "valDisponivel", "valReservado", "valCanceladoReserva", "valReservadoLiquido", "valTotalEmpenhado",
            "valAnuladoEmpenho", "valEmpenhadoLiquido", "valLiquidado", "valPagoExercicio", "valPagoRestos",
            "saldo_dotacao", "data_hora_extracao"
        ]

        colunas_execucao = [c for c in colunas_execucao if c in df_final.columns]
        colunas_restantes = [c for c in df_final.columns if c not in colunas_execucao]
        df_final = df_final[colunas_execucao + colunas_restantes]
        df_final.to_excel(os.path.join(BASE_EXEC, "execucao.xlsx"), index=False)
    else:
        print("\n⚠️ AVISO CRÍTICO: A API não retornou dados de despesas. O arquivo local não será sobrescrito para proteger a base histórica.")

    # -------------------------------------------------------------
    # 2. CONSULTA DE EMPENHOS
    # -------------------------------------------------------------
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

    params_empenhos = {"anoEmpenho": ano, "mesEmpenho": mes, "numCpfCnpj": CNPJ, "numPagina": 1}

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
    
    for pagina in range(1, total_paginas + 1):
        params_empenhos["numPagina"] = pagina
        resposta_empenhos = fazer_requisicao("empenhos", params=params_empenhos, token=TOKEN)
        if not resposta_empenhos or "lstEmpenhos" not in resposta_empenhos:
            continue
        
        df_empenhos = pd.json_normalize(resposta_empenhos["lstEmpenhos"])
        if df_empenhos.empty:
            continue

        df_empenhos["dotacao_completa"] = (
            col_str(df_empenhos, "codOrgao")+ "." + col_str(df_empenhos, "codUnidade")+ "." +
            col_str(df_empenhos, "codFuncao")+ "." + col_str(df_empenhos, "codSubFuncao")+ "." +
            col_str(df_empenhos, "codPrograma")+ "." + col_str(df_empenhos, "codProjetoAtividade")+ "." +
            col_str(df_empenhos, "codCategoria") + col_str(df_empenhos, "codGrupo") +
            col_str(df_empenhos, "codModalidade") + col_str(df_empenhos, "codElemento") + "00." +
            col_str(df_empenhos, "codFonteRecurso")
        )

        codproc = df_empenhos["codProcesso"].astype("string").fillna("").str.replace(r"\D", "", regex=True)
        codproc = codproc.str.zfill(16)
        has_codproc = codproc.str.len() == 16
        df_empenhos.loc[has_codproc, "codProcesso"] = (
            codproc.str.slice(0, 4) + "." + codproc.str.slice(4, 8) + "/" + codproc.str.slice(8, 15) + "-" + codproc.str.slice(15)
        )

        if "anexos" in df_empenhos.columns:
            anexos_extraidos = df_empenhos["anexos"].apply(extrai_anexo).apply(pd.Series).add_prefix("anexo_")
            df_empenhos = pd.concat([df_empenhos, anexos_extraidos], axis=1).drop(columns=["anexos"], errors='ignore')

        list_empenhos.append(df_empenhos)

    df_final_empenhos = pd.concat(list_empenhos, ignore_index=True) if list_empenhos else pd.DataFrame()

    sucesso_empenhos = not df_final_empenhos.empty

    if sucesso_empenhos:
        def get_sigla_for_empenho(row):
            try:
                orgao = str(row['codOrgao']).zfill(2)
                proj_ativ = str(row['codProjetoAtividade']).zfill(4) if 'codProjetoAtividade' in row else ''
                if proj_ativ:
                    chave_completa = f"{orgao}_{proj_ativ}"
                    sigla = lookup_orgao_proj_ativ_sigla.get(chave_completa, '')
                    if sigla:
                        return sigla
                return lookup_orgao_sigla.get(orgao, '')
            except:
                return ''
        
        df_final_empenhos['sigla_orgao'] = df_final_empenhos.apply(get_sigla_for_empenho, axis=1)
        df_final_empenhos['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
        df_final_empenhos.to_excel(os.path.join(BASE_EXEC, "empenhos.xlsx"), index=False)
    else:
        print("\n⚠️ AVISO CRÍTICO: A API não retornou dados de empenhos válidos para hoje. O arquivo não será sobrescrito.")

    # -------------------------------------------------------------
    # 3. UPLOAD OBRIGATÓRIO PARA O GOOGLE DRIVE 
    # -------------------------------------------------------------
    if drive_service and DRIVE_FOLDER_IDS:
        print("\n" + "="*60)
        print("SINCROZINANDO BASES ATUALIZADAS COM O GOOGLE DRIVE")
        print("="*60)
        for folder_id in DRIVE_FOLDER_IDS:
            print(f"\n📁 Sincronizando com a pasta: {folder_id}")
            if sucesso_execucao:
                upload_or_update_file(drive_service, os.path.join(BASE_EXEC, "execucao.xlsx"), folder_id)
            if sucesso_empenhos:
                upload_or_update_file(drive_service, os.path.join(BASE_EXEC, "empenhos.xlsx"), folder_id)
    else:
        print("\nAviso: Pastas do Google Drive não configuradas. Upload das bases pulado.")

    # -------------------------------------------------------------
    # 4. COMPARAÇÃO E RELATÓRIO DE MUDANÇAS - EXECUÇÃO (Uso do Disponível)
    # -------------------------------------------------------------
    if sucesso_execucao:
        print("\n" + "="*60)
        print("ANÁLISE DE MUDANÇAS - EXECUÇÃO")
        print("="*60)

        despesas_anterior_norm = normalizar_para_comparacao(despesas_anterior)
        df_final_norm = normalizar_para_comparacao(df_final)

        colunas_comuns = list(set(despesas_anterior_norm.columns) & set(df_final_norm.columns)) if not despesas_anterior_norm.empty else list(df_final_norm.columns)

        anterior_por_dotacao = {str(row.get('dotacao', '')): row for idx, row in despesas_anterior_norm.iterrows()} if not despesas_anterior_norm.empty else {}
        final_por_dotacao = {str(row.get('dotacao', '')): row for idx, row in df_final_norm.iterrows()}

        mudancas_exec = []

        # Linhas adicionadas
        dotacoes_adicionadas = set(final_por_dotacao.keys()) - set(anterior_por_dotacao.keys())
        if len(dotacoes_adicionadas) > 0:
            print(f"\n✅ {len(dotacoes_adicionadas)} dotações ADICIONADAS")
            for dotacao in dotacoes_adicionadas:
                row = final_por_dotacao[dotacao]
                if not row.get('dotacao_exclusiva'):
                    continue
                mudancas_exec.append({
                    "tipo_mudanca": "ADICIONADA",
                    "dotacao": dotacao,
                    "dotacao_exclusiva": row.get('dotacao_exclusiva', ''),
                    "coluna": "valDisponivel",
                    "valor_anterior": 0,
                    "valor_novo": row.get('valDisponivel', 0),
                    "detalhes": "Nova dotação adicionada à base"
                })

        # Linhas modificadas (Agrupamento das colunas sob o guarda-chuva do 'Disponível')
        dotacoes_comuns = set(anterior_por_dotacao.keys()) & set(final_por_dotacao.keys())
        linhas_modificadas = []

        for dotacao in dotacoes_comuns:
            linha_anterior = anterior_por_dotacao[dotacao]
            linha_final = final_por_dotacao[dotacao]

            if not_linha_final := not linha_final.get('dotacao_exclusiva'):
                continue

            def get_val_numeric(linha, col):
                val = linha.get(col, 0)
                if pd.isna(val) or val == '' or val is None:
                    return 0.0
                try:
                    return float(val)
                except:
                    return 0.0

            val_ant_disp = get_val_numeric(linha_anterior, 'valDisponivel')
            val_nov_disp = get_val_numeric(linha_final, 'valDisponivel')
            
            val_ant_supl = get_val_numeric(linha_anterior, 'valSuplementado')
            val_nov_supl = get_val_numeric(linha_final, 'valSuplementado')
            
            val_ant_red = get_val_numeric(linha_anterior, 'valReduzido')
            val_nov_red = get_val_numeric(linha_final, 'valReduzido')
            
            val_ant_cong = get_val_numeric(linha_anterior, 'valCongelado')
            val_nov_cong = get_val_numeric(linha_final, 'valCongelado')
            
            val_ant_desc = get_val_numeric(linha_anterior, 'valDescongelado')
            val_nov_desc = get_val_numeric(linha_final, 'valDescongelado')

            mudou_disponivel = round(val_ant_disp, 2) != round(val_nov_disp, 2)
            mudou_suplementado = round(val_ant_supl, 2) != round(val_nov_supl, 2)
            mudou_reduzido = round(val_ant_red, 2) != round(val_nov_red, 2)
            mudou_congelado = round(val_ant_cong, 2) != round(val_nov_cong, 2)
            mudou_descongelado = round(val_ant_desc, 2) != round(val_nov_desc, 2)

            if mudou_disponivel or mudou_suplementado or mudou_reduzido or mudou_congelado or mudou_descongelado:
                alteracoes = []
                if mudou_suplementado:
                    var_supl = abs(val_nov_supl - val_ant_supl)
                    alteracoes.append(f"Suplementado ({formatar_brl(var_supl)})")
                if mudou_reduzido:
                    var_red = abs(val_nov_red - val_ant_red)
                    alteracoes.append(f"Reduzido ({formatar_brl(var_red)})")
                if mudou_congelado:
                    var_cong = abs(val_nov_cong - val_ant_cong)
                    alteracoes.append(f"Congelado ({formatar_brl(var_cong)})")
                if mudou_descongelado:
                    var_desc = abs(val_nov_desc - val_ant_desc)
                    alteracoes.append(f"Descongelado ({formatar_brl(var_desc)})")

                detalhes_outros = "; ".join(alteracoes) if alteracoes else "Apenas Disponível alterado"

                linhas_modificadas.append({
                    "dotacao": dotacao,
                    "dotacao_exclusiva": linha_final.get('dotacao_exclusiva', ''),
                    "coluna": "valDisponivel",
                    "valor_anterior": val_ant_disp,
                    "valor_novo": val_nov_disp,
                    "detalhes": detalhes_outros
                })

        if len(linhas_modificadas) > 0:
            print(f"\n🔄 {len(linhas_modificadas)} MUDANÇAS DE VALORES")
            for mudanca in linhas_modificadas:
                mudancas_exec.append({
                    "tipo_mudanca": "MODIFICADA",
                    "dotacao": mudanca['dotacao'],
                    "dotacao_exclusiva": mudanca['dotacao_exclusiva'],
                    "coluna": mudanca['coluna'],
                    "valor_anterior": mudanca['valor_anterior'],
                    "valor_novo": mudanca['valor_novo'],
                    "detalhes": mudanca['detalhes']
                })

        if mudancas_exec:
            df_mudancas = pd.DataFrame(mudancas_exec)
            df_mudancas['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
            
            df_mudancas['valor_anterior'] = df_mudancas['valor_anterior'].apply(lambda x: '-' if pd.isna(x) else x)
            df_mudancas['valor_novo'] = df_mudancas['valor_novo'].apply(lambda x: '-' if pd.isna(x) else x)
            
            def get_sigla_from_dotacao(dotacao_str):
                if pd.isna(dotacao_str) or dotacao_str == '':
                    return ''
                try:
                    partes = dotacao_str.split('.')
                    if len(partes) < 6:
                        orgao = partes[0] if len(partes) > 0 else ''
                        return lookup_orgao_sigla.get(orgao, '')
                    orgao = partes[0]
                    proj_ativ = partes[5]
                    chave_completa = f"{orgao}_{proj_ativ}"
                    sigla = lookup_orgao_proj_ativ_sigla.get(chave_completa, '')
                    if not sigla:
                        sigla = lookup_orgao_sigla.get(orgao, '')
                    return sigla
                except:
                    return ''
            
            df_mudancas['sigla_orgao'] = df_mudancas['dotacao'].apply(get_sigla_from_dotacao)
            
            df_mudancas = df_mudancas.rename(columns={
                "sigla_orgao": "Sigla Órgão",
                "tipo_mudanca": "Tipo de Mudança",
                "dotacao": "Dotação",
                "dotacao_exclusiva": "Dotação Exclusiva",
                "coluna": "Campo Alterado",
                "valor_anterior": "Valor Anterior",
                "valor_novo": "Valor Atualizado",
                "detalhes": "Detalhes",
                "data_hora_extracao": "Data/Hora Extração"
            })
            
            colunas_ordenadas = ['Sigla Órgão', 'Tipo de Mudança', 'Dotação', 'Dotação Exclusiva', 'Campo Alterado', 'Valor Anterior', 'Valor Atualizado', 'Detalhes', 'Data/Hora Extração']
            colunas_existentes = [c for c in colunas_ordenadas if c in df_mudancas.columns]
            colunas_extra = [c for c in df_mudancas.columns if c not in colunas_existentes]
            df_mudancas = df_mudancas[colunas_existentes + colunas_extra]
            
            df_mudancas['Valor Anterior'] = df_mudancas['Valor Anterior'].apply(formatar_brl)
            df_mudancas['Valor Atualizado'] = df_mudancas['Valor Atualizado'].apply(formatar_brl)
            
            df_mudancas.to_excel(os.path.join(BASE_EXEC, "mudancas_execucao.xlsx"), index=False)
            print(f"\n📊 Relatório salvo em: mudancas_execucao.xlsx")
        else:
            print("\n✨ Nenhuma mudança detectada na execução!")

    # -------------------------------------------------------------
    # 5. COMPARAÇÃO E RELATÓRIO DE MUDANÇAS - EMPENHOS
    # -------------------------------------------------------------
    if sucesso_empenhos:
        print("\n" + "="*60)
        print("ANÁLISE DE MUDANÇAS - EMPENHOS")
        print("="*60)

        empenhos_anterior_norm = normalizar_para_comparacao(empenhos_anterior)
        df_final_empenhos_norm = normalizar_para_comparacao(df_final_empenhos)

        colunas_comuns_emp = list(set(empenhos_anterior_norm.columns) & set(df_final_empenhos_norm.columns)) if not empenhos_anterior_norm.empty else list(df_final_empenhos_norm.columns)

        def format_emp_key(row):
            """Cria chave composta para evitar sobrescrita de itens de um mesmo empenho."""
            cod = str(row.get('codEmpenho', '')).strip()
            if cod.endswith('.0'): cod = cod[:-2]
            dot = str(row.get('dotacao_completa', '')).strip()
            return f"{cod}_{dot}"

        anterior_emp_dict = {format_emp_key(row): row.to_dict() for idx, row in empenhos_anterior_norm.iterrows()} if not empenhos_anterior_norm.empty else {}
        final_emp_dict = {format_emp_key(row): row.to_dict() for idx, row in df_final_empenhos_norm.iterrows()} if not df_final_empenhos_norm.empty else {}

        mudancas_emp = []

        ids_adicionados = set(final_emp_dict.keys()) - set(anterior_emp_dict.keys())
        if ids_adicionados:
            print(f"\n✅ {len(ids_adicionados)} empenhos/itens ADICIONADOS")
            for dict_key in ids_adicionados:
                row = final_emp_dict[dict_key]
                mudancas_emp.append({
                    "numProcesso": row.get('codProcesso', ''),
                    "dotacao": row.get('dotacao_completa', ''),
                    "codEmpenho": row.get('codEmpenho', ''),
                    "numeroOriginalContrato": row.get('numeroOriginalContrato', ''),
                    "coluna": "valEmpenhadoLiquido",
                    "valor_anterior": 0,
                    "valor_novo": row.get('valTotalEmpenhado', 0)
                })

        ids_comuns = set(anterior_emp_dict.keys()) & set(final_emp_dict.keys())
        for dict_key in ids_comuns:
            linha_ant = anterior_emp_dict[dict_key]
            linha_fin = final_emp_dict[dict_key]
            
            for col in colunas_comuns_emp:
                if not col.startswith('val') or col == 'data_hora_extracao':
                    continue
                    
                val_ant = linha_ant.get(col)
                val_fin = linha_fin.get(col)
                
                if val_ant != val_fin:
                    mudancas_emp.append({
                        "dotacao": linha_fin.get('dotacao_completa', ''),
                        "codEmpenho": linha_fin.get('codEmpenho', ''),
                        "numProcesso": linha_fin.get('codProcesso', ''),
                        "numeroOriginalContrato": linha_fin.get('numeroOriginalContrato', ''),
                        "coluna": col,
                        "valor_anterior": val_ant,
                        "valor_novo": val_fin,
                    })

        if mudancas_emp:
            df_mudancas_emp = pd.DataFrame(mudancas_emp)
            df_mudancas_emp['data_hora_extracao'] = dt_inicio.strftime('%d/%m/%Y %H:%M:%S')
            
            df_mudancas_emp['valor_anterior'] = df_mudancas_emp['valor_anterior'].apply(lambda x: '-' if pd.isna(x) else x)
            df_mudancas_emp['valor_novo'] = df_mudancas_emp['valor_novo'].apply(lambda x: '-' if pd.isna(x) else x)
            
            def get_sigla_from_dotacao_emp(dotacao_str):
                if pd.isna(dotacao_str) or dotacao_str == '':
                    return ''
                try:
                    partes = dotacao_str.split('.')
                    if len(partes) < 6:
                        orgao = partes[0] if len(partes) > 0 else ''
                        return lookup_orgao_sigla.get(orgao, '')
                    orgao = partes[0]
                    proj_ativ = partes[5]
                    chave_completa = f"{orgao}_{proj_ativ}"
                    sigla = lookup_orgao_proj_ativ_sigla.get(chave_completa, '')
                    if not sigla:
                        sigla = lookup_orgao_sigla.get(orgao, '')
                    return sigla
                except:
                    return ''
            
            df_mudancas_emp['sigla_orgao'] = df_mudancas_emp['dotacao'].apply(get_sigla_from_dotacao_emp)
            
            df_mudancas_emp = df_mudancas_emp.rename(columns={
                "sigla_orgao": "Sigla Órgão",
                "numProcesso": "Processo SEI",
                "dotacao": "Dotação",
                "codEmpenho": "Código do Empenho",
                "numeroOriginalContrato": "Número do Contrato",
                "coluna": "Campo Alterado",
                "valor_anterior": "Valor Anterior",
                "valor_novo": "Valor Atualizado",
                "data_hora_extracao": "Data/Hora Extração"
            })
            
            colunas_ordenadas = ['Sigla Órgão', 'Processo SEI', 'Dotação', 'Código do Empenho', 'Número do Contrato', 'Campo Alterado', 'Valor Anterior', 'Valor Atualizado', 'Data/Hora Extração']
            colunas_existentes = [c for c in colunas_ordenadas if c in df_mudancas_emp.columns]
            colunas_extra = [c for c in df_mudancas_emp.columns if c not in colunas_existentes]
            df_mudancas_emp = df_mudancas_emp[colunas_existentes + colunas_extra]
            
            df_mudancas_emp['Valor Anterior'] = df_mudancas_emp['Valor Anterior'].apply(formatar_brl)
            df_mudancas_emp['Valor Atualizado'] = df_mudancas_emp['Valor Atualizado'].apply(formatar_brl)
            
            df_mudancas_emp.to_excel(os.path.join(BASE_EXEC, "mudancas_empenhos.xlsx"), index=False)
            print(f"\n📊 Relatório salvo em: mudancas_empenhos.xlsx")
        else:
            print("\n✨ Nenhuma mudança detectada nos empenhos!")

    print("="*60)

if __name__ == "__main__":
    main()