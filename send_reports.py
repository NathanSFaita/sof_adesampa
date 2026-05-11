import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from datetime import datetime
from consulta_sof import input_with_timeout
import pytz

# Google Drive
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Configuração de fuso horário
tz_brasilia = pytz.timezone('America/Sao_Paulo')

# --- Configurações de Caminho ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_EXEC = os.path.join(BASE_PATH, "base_execucao")
AUX_FILES_PATH = os.path.join(BASE_PATH, "arquivos_auxiliares")
EMAILS_FILE = os.path.join(AUX_FILES_PATH, "emails.xlsx")

# --- Variáveis de Ambiente para E-mail ---
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")  # Padrão para Gmail
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))  # Padrão para TLS

# --- Variáveis de Ambiente para Google Drive ---
SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    os.path.join(BASE_PATH, "service_account.json")
)
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")

if not EMAIL_SENDER:
    print("AVISO: Variável de ambiente EMAIL_SENDER não configurada. Insira manualmente.")
    EMAIL_SENDER = input_with_timeout("Digite o e-mail do remetente (EMAIL_SENDER): ", timeout=30)
    EMAIL_PASSWORD = input_with_timeout("Digite a senha do remetente (EMAIL_PASSWORD): ", timeout=30)
    DRIVE_FOLDER_ID = input_with_timeout("Digite o ID da pasta do Google Drive para upload (DRIVE_FOLDER_ID): ", timeout=30)

# --- Funções Auxiliares ---

def get_recipients(emails_file_path):
    """
    Lê os endereços de e-mail dos destinatários de um arquivo Excel.
    O arquivo deve ter uma coluna chamada 'email'.
    """
    try:
        df_emails = pd.read_excel(emails_file_path)
        valid_recipients = []
        for _, row in df_emails.iterrows():
            email = str(row.get('email', '')).strip()
            if '@' in email:
                valid_recipients.append({
                    'email': email,
                    'nome': str(row.get('nome', 'Destinatário')).strip(),
                    'genero': str(row.get('genero', 'M')).strip().upper()
                })
        return valid_recipients
    except FileNotFoundError:
        print(f"Erro: Arquivo de e-mails não encontrado em {emails_file_path}. Nenhum e-mail será enviado.")
        return []
    except KeyError:
        print(f"Erro: O arquivo {emails_file_path} não possui a coluna 'email'. Nenhum e-mail será enviado.")
        return []
    except Exception as e:
        print(f"Erro ao ler arquivo de e-mails {emails_file_path}: {e}. Nenhum e-mail será enviado.")
        return []

def formatar_brl_email(valor):
    """Formata um valor numérico para o formato BRL (R$ x.xxx,xx)"""
    if pd.isna(valor) or valor == '' or valor == 'nan' or valor == '-':
        return valor if valor == '-' else '-'
    try:
        if isinstance(valor, str) and valor.startswith('R$'):
            return valor
        num = float(str(valor).replace('.', '').replace(',', '.'))
        return f"R$ {num:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.')
    except:
        return str(valor)

def prepare_html_body(base_exec_path):
    """
    Prepara o corpo do e-mail em formato HTML com as tabelas de mudanças.
    """
    has_changes = False
    today_str = datetime.now(tz_brasilia).strftime('%d/%m/%Y')
    tables_html = ""
    
    table_style = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:12px;"'
    th_style = 'style="background-color:#003366;color:white;padding:12px;text-align:left;border:1px solid #ddd;"'
    td_style = 'style="padding:8px 12px;border:1px solid #ddd;"'

    # --- Mudanças de Despesas (Execução Orçamentária) ---
    path_mudancas_exec = os.path.join(base_exec_path, "mudancas_execucao.xlsx")
    if os.path.exists(path_mudancas_exec):
        try:
            df_mudancas_exec = pd.read_excel(path_mudancas_exec)
            if not df_mudancas_exec.empty and 'Data/Hora Extração' in df_mudancas_exec.columns:
                report_date = str(df_mudancas_exec['Data/Hora Extração'].iloc[0]).split(' ')[0]
                if report_date == today_str:
                    has_changes = True
                    tables_html += "<h3 style='color: #003366;'>📊 Mudanças na Execução Orçamentária dos Contratos de Gestão</h3>"
                    df_display = df_mudancas_exec.drop(columns=['Data/Hora Extração'], errors='ignore').copy()
                    colunas_ordenadas = [
                        'Sigla Órgão',
                        'Tipo de Mudança',
                        'Dotação',
                        'Dotação Exclusiva',
                        'Campo Alterado',
                        'Valor Anterior',
                        'Valor Atualizado',
                        'Detalhes'
                    ]
                    colunas_existentes = [c for c in colunas_ordenadas if c in df_display.columns]
                    df_display = df_display[colunas_existentes + [c for c in df_display.columns if c not in colunas_existentes]]
                    for col in df_display.columns:
                        if any(x in col.lower() for x in ['valor', 'saldo']):
                            df_display[col] = df_display[col].apply(formatar_brl_email)
                    html_table = df_display.to_html(index=False, border=1, escape=False, render_links=False, table_id=None, justify='center', classes=None, header=True, na_rep='')
                    html_table = html_table.replace('<table', f'<table {table_style}', 1)
                    html_table = html_table.replace('<th>', f'<th {th_style}>').replace('<td>', f'<td {td_style}>')
                    tables_html += html_table
                else:
                    tables_html += "<p><strong>Despesas (Execução Orçamentária):</strong> Nenhuma mudança detectada hoje.</p>"
            else:
                tables_html += "<p><strong>Despesas (Execução Orçamentária):</strong> Nenhuma mudança detectada.</p>"
        except Exception as e:
            tables_html += f"<p><strong>Despesas (Execução Orçamentária):</strong> Erro ao carregar relatório - {e}</p>"
    else:
        tables_html += "<p><strong>Despesas (Execução Orçamentária):</strong> Relatório não encontrado.</p>"

    # --- Mudanças de Empenhos ---
    path_mudancas_empenhos = os.path.join(base_exec_path, "mudancas_empenhos.xlsx")
    if os.path.exists(path_mudancas_empenhos):
        try:
            df_mudancas_empenhos = pd.read_excel(path_mudancas_empenhos)
            if not df_mudancas_empenhos.empty and 'Data/Hora Extração' in df_mudancas_empenhos.columns:
                report_date = str(df_mudancas_empenhos['Data/Hora Extração'].iloc[0]).split(' ')[0]
                if report_date == today_str:
                    has_changes = True
                    tables_html += "<h3 style='color: #003366;'>📋 Mudanças nos Empenhos</h3>"
                    df_display = df_mudancas_empenhos.drop(columns=['Data/Hora Extração'], errors='ignore').copy()
                    colunas_ordenadas = [
                        'Sigla Órgão',
                        'Processo SEI',
                        'Tipo de Mudança',
                        'Dotação',
                        'Código do Empenho',
                        'Número do Contrato',
                        'Campo Alterado',
                        'Valor Anterior',
                        'Valor Atualizado',
                        'Detalhes'
                    ]
                    colunas_existentes = [c for c in colunas_ordenadas if c in df_display.columns]
                    df_display = df_display[colunas_existentes + [c for c in df_display.columns if c not in colunas_existentes]]
                    for col in df_display.columns:
                        if any(x in col.lower() for x in ['valor', 'saldo']):
                            df_display[col] = df_display[col].apply(formatar_brl_email)
                    html_table = df_display.to_html(index=False, border=1, escape=False, render_links=False, table_id=None, justify='center', classes=None, header=True, na_rep='')
                    html_table = html_table.replace('<table', f'<table {table_style}', 1)
                    html_table = html_table.replace('<th>', f'<th {th_style}>').replace('<td>', f'<td {td_style}>')
                    tables_html += html_table
                else:
                    tables_html += "<p><strong>Empenhos:</strong> Nenhuma mudança detectada hoje.</p>"
            else:
                tables_html += "<p><strong>Empenhos:</strong> Nenhuma mudança detectada.</p>"
        except Exception as e:
            tables_html += f"<p><strong>Empenhos:</strong> Erro ao carregar relatório - {e}</p>"
    else:
        tables_html += "<p><strong>Empenhos:</strong> Relatório não encontrado.</p>"

    return tables_html, has_changes

def attach_file(message, filepath):
    """Anexa um arquivo ao objeto de mensagem de e-mail."""
    if not os.path.exists(filepath):
        print(f"Aviso: Arquivo não encontrado para anexar: {filepath}")
        return
    try:
        with open(filepath, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(filepath)}",
        )
        message.attach(part)
    except Exception as e:
        print(f"Erro ao anexar arquivo {filepath}: {e}")

def attach_signature_image(message, image_path):
    """Anexa a imagem de assinatura ao e-mail com Content-ID para referência no HTML."""
    if not os.path.exists(image_path):
        print(f"Aviso: Imagem de assinatura não encontrada: {image_path}")
        return False
    try:
        with open(image_path, "rb") as img_file:
            img = MIMEImage(img_file.read())
            img.add_header('Content-ID', '<signature>')
            img.add_header('Content-Disposition', 'inline', filename='assinatura.png')
            message.attach(img)
        return True
    except Exception as e:
        print(f"Erro ao anexar imagem de assinatura {image_path}: {e}")
        return False

# --- Google Drive ---   
SCOPES = ["https://www.googleapis.com/auth/drive"]

def build_drive_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"Erro: serviço de conta Google não encontrado em {SERVICE_ACCOUNT_FILE}")
        return None
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"Erro ao criar serviço do Google Drive: {e}")
        return None

def get_file_in_folder(service, file_name, folder_id):
    # Para Shared Drives, assumindo que folder_id é o ID do drive
    try:
        response = service.files().list(
            q=f"name = '{file_name}' and trashed = false",
            corpora='drive',
            driveId=folder_id,
            fields="files(id, name)",
            pageSize=1,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True  # Adicionado
        ).execute()
        files = response.get("files", [])
        print(f"Buscando {file_name} no folder {folder_id}: encontrou {len(files)} arquivos")
        return files[0]["id"] if files else None
    except Exception as e:
        print(f"Erro ao buscar arquivo no Drive ({file_name}): {e}")
        return None

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
                    print(f"Arquivo {file_name} não encontrado para update, criando novo...")
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

def upload_reports_to_drive(file_paths):
    if not DRIVE_FOLDER_ID:
        print("Drive não configurado: variável de ambiente DRIVE_FOLDER_ID ausente.")
        return
    service = build_drive_service()
    if not service:
        return
    for path in file_paths:
        upload_or_update_file(service, path, DRIVE_FOLDER_ID)

# --- Função Principal de Envio de E-mail ---

def send_reports_email():
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT]):
        print("ERRO CRÍTICO: Variáveis de ambiente de e-mail não configuradas. Verifique EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT.")
        return

    recipients_data = get_recipients(EMAILS_FILE)
    if not recipients_data:
        print("ERRO CRÍTICO: Nenhum destinatário válido encontrado. E-mail não será enviado.")
        return

    tables_content, has_changes = prepare_html_body(BASE_EXEC)

    if not has_changes:
        print("Nenhuma mudança detectada nos arquivos. O e-mail não será enviado.")
        return

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            for recipient in recipients_data:
                nome = recipient['nome']
                email = recipient['email']
                genero = recipient['genero']

                saudacao = "Prezada" if genero == "F" else "Prezado"
                
                msg = MIMEMultipart("alternative")
                msg['From'] = EMAIL_SENDER
                msg['To'] = email
                msg['Subject'] = f"Relatório Atualizado SOF - {datetime.now().strftime('%d/%m/%Y')}"

                html_body = f"""
                <html>
                <body style="font-family: Arial, sans-serif;">
                    <p>{saudacao} {nome},</p>
                    <p>Informo que houveram mudanças na execução orçamentária dos contratos de gestão, conforme está informado nas tabelas abaixo. 
                    Segue anexo os arquivos atualizados contendo a situação das dotações orçamentárias dos contratos de gestão e dos nossos empenhos</p>
                    {tables_content}
                    <p>Atenciosamente,<br>
                    <img src="cid:signature" style="max-width: 500px; height: auto;">
                    </p>
                </body>
                </html>
                """
                msg.attach(MIMEText(html_body, 'html', 'utf-8'))

                signature_path = os.path.join(AUX_FILES_PATH, "assinatura.png")
                attach_signature_image(msg, signature_path)

                attach_file(msg, os.path.join(BASE_EXEC, "execucao.xlsx"))
                attach_file(msg, os.path.join(BASE_EXEC, "empenhos.xlsx"))

                server.send_message(msg)
                print(f"E-mail enviado com sucesso para: {email}")

        # Upload para Google Drive após o envio de e-mail
        upload_reports_to_drive([
            os.path.join(BASE_EXEC, "execucao.xlsx"),
            os.path.join(BASE_EXEC, "empenhos.xlsx"),
        ])

    except smtplib.SMTPAuthenticationError:
        print("ERRO: Falha na autenticação SMTP. Verifique o EMAIL_SENDER e EMAIL_PASSWORD.")
    except smtplib.SMTPConnectError as e:
        print(f"ERRO: Falha ao conectar ao servidor SMTP. Verifique EMAIL_SMTP_SERVER e EMAIL_SMTP_PORT. Detalhes: {e}")
    except Exception as e:
        print(f"ERRO INESPERADO ao enviar e-mail: {e}")

if __name__ == "__main__":
    send_reports_email()
