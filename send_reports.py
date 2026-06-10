import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from datetime import datetime
import pytz

# Reaproveitando inputs e funções auxiliares unificadas
from consulta_sof import input_with_timeout, formatar_brl

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

if not EMAIL_SENDER:
    print("AVISO: Variável de ambiente EMAIL_SENDER não configurada. Insira manualmente.")
    EMAIL_SENDER = input_with_timeout("Digite o e-mail do remetente (EMAIL_SENDER): ", timeout=30)
    EMAIL_PASSWORD = input_with_timeout("Digite a senha do remetente (EMAIL_PASSWORD): ", timeout=30)

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

def get_report_date(file_path):
    """Retorna a data do relatório no formato dd/mm/AAAA ou None."""
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_excel(file_path)
        if df.empty or 'Data/Hora Extração' not in df.columns:
            return None
        value = df['Data/Hora Extração'].iloc[0]
        if pd.isna(value):
            return None
            
        # Se o pandas já leu como objeto datetime do python/pandas
        if isinstance(value, pd.Timestamp) or isinstance(value, datetime):
            return value.strftime('%d/%m/%Y')
            
        # Se for string, tentamos converter garantindo que o dia vem primeiro (padrão BR)
        val_str = str(value).strip()
        try:
            dt = pd.to_datetime(val_str, dayfirst=True)
            return dt.strftime('%d/%m/%Y')
        except Exception:
            # Fallback: tenta pegar apenas a parte da data caso seja uma string estranha
            return val_str.split(' ')[0]
    except Exception as e:
        print(f"Erro ao ler a data do relatório {file_path}: {e}")
        return None

def prepare_html_body(base_exec_path, include_execucao=True, include_empenhos=True):
    """
    Prepara o corpo do e-mail em formato HTML com as tabelas de mudanças.
    """
    has_changes = False
    today_str = datetime.now(tz_brasilia).strftime('%d/%m/%Y')
    tables_html = ""
    
    # NOVOS ESTILOS: Tabela levemente maior (font-size 13px, min-width 800px) e com mais espaçamento
    table_style = 'style="border-collapse:collapse;width:100%;min-width:800px;font-family:\'Segoe UI\', Arial, sans-serif;font-size:13px; margin-bottom: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); border-radius: 6px; overflow: hidden;"'
    th_style = 'style="background-color:#003366;color:white;padding:14px 10px;text-align:center;font-weight:600;letter-spacing:0.5px; border:none;"'
    td_style = 'style="padding:12px 10px;border-bottom:1px solid #f0f0f0;color:#444444;text-align:center; border-left:none; border-right:none; word-break: break-word;"'

    # --- Mudanças de Despesas (Execução Orçamentária) ---
    path_mudancas_exec = os.path.join(base_exec_path, "mudancas_execucao.xlsx")
    if include_execucao and os.path.exists(path_mudancas_exec):
        try:
            df_mudancas_exec = pd.read_excel(path_mudancas_exec)
            if not df_mudancas_exec.empty and 'Data/Hora Extração' in df_mudancas_exec.columns:
                report_date = get_report_date(path_mudancas_exec)
                if report_date == today_str:
                    has_changes = True
                    # Título mais limpo
                    tables_html += "<h3 style='color: #003366; padding-bottom: 5px; margin-top: 30px; font-family: Arial, sans-serif; font-size: 16px;'>📊 Mudanças na Execução Orçamentária</h3>"
                    
                    # REMOÇÃO DAS COLUNAS: 'Tipo de Mudança' e 'Dotação Exclusiva' e 'Data/Hora'
                    df_display = df_mudancas_exec.drop(columns=['Data/Hora Extração', 'Tipo de Mudança', 'Dotação Exclusiva'], errors='ignore').copy()
                    
                    # Colunas do Relatório reestruturadas
                    colunas_ordenadas = [
                        'Sigla Órgão', 'Dotação', 'Campo Alterado', 'Valor Anterior', 'Valor Atualizado', 'Detalhes'
                    ]
                    colunas_existentes = [c for c in colunas_ordenadas if c in df_display.columns]
                    df_display = df_display[colunas_existentes + [c for c in df_display.columns if c not in colunas_existentes]]
                    
                    for col in df_display.columns:
                        if any(x in col.lower() for x in ['valor', 'saldo']):
                            df_display[col] = df_display[col].apply(formatar_brl)
                            
                    # border=0 remove as bordas default feias do HTML gerado pelo Pandas
                    html_table = df_display.to_html(index=False, border=0, escape=False, render_links=False, justify='center', header=True, na_rep='')
                    html_table = html_table.replace('<table', f'<table {table_style}', 1)
                    html_table = html_table.replace('<th>', f'<th {th_style}>').replace('<td>', f'<td {td_style}>')
                    # Wrapper com overflow-x para não estourar o e-mail em telas menores
                    tables_html += f'<div style="overflow-x: auto; width: 100%;">{html_table}</div>'
                else:
                    tables_html += "<p style='color: #666;'><strong>Despesas (Execução Orçamentária):</strong> Nenhuma mudança detectada hoje.</p>"
            else:
                tables_html += "<p style='color: #666;'><strong>Despesas (Execução Orçamentária):</strong> Nenhuma mudança detectada.</p>"
        except Exception as e:
            tables_html += f"<p style='color: #cc0000;'><strong>Despesas (Execução Orçamentária):</strong> Erro ao carregar relatório - {e}</p>"
    else:
        if include_execucao:
            tables_html += "<p style='color: #666;'><strong>Despesas (Execução Orçamentária):</strong> Relatório não encontrado.</p>"

    # --- Mudanças de Empenhos ---
    path_mudancas_empenhos = os.path.join(base_exec_path, "mudancas_empenhos.xlsx")
    if include_empenhos and os.path.exists(path_mudancas_empenhos):
        try:
            df_mudancas_empenhos = pd.read_excel(path_mudancas_empenhos)
            if not df_mudancas_empenhos.empty and 'Data/Hora Extração' in df_mudancas_empenhos.columns:
                report_date = get_report_date(path_mudancas_empenhos)
                if report_date == today_str:
                    has_changes = True
                    tables_html += "<h3 style='color: #003366; padding-bottom: 5px; margin-top: 40px; font-family: Arial, sans-serif; font-size: 16px;'>📋 Mudanças nos Empenhos</h3>"
                    # Remoção das colunas desnecessárias (Código do Empenho e Número do Contrato ocultados)
                    df_display = df_mudancas_empenhos.drop(columns=['Data/Hora Extração', 'Detalhes', 'Código do Empenho', 'Número do Contrato'], errors='ignore').copy()
                    
                    # Colunas reordenadas sem Código do Empenho e Número do Contrato
                    colunas_ordenadas = [
                        'Sigla Órgão', 'Processo SEI', 'Dotação', 'Campo Alterado', 'Valor Anterior', 'Valor Atualizado'
                    ]
                    colunas_existentes = [c for c in colunas_ordenadas if c in df_display.columns]
                    df_display = df_display[colunas_existentes + [c for c in df_display.columns if c not in colunas_existentes]]
                    
                    for col in df_display.columns:
                        if any(x in col.lower() for x in ['valor', 'saldo']):
                            df_display[col] = df_display[col].apply(formatar_brl)
                            
                    html_table = df_display.to_html(index=False, border=0, escape=False, render_links=False, justify='center', header=True, na_rep='')
                    html_table = html_table.replace('<table', f'<table {table_style}', 1)
                    html_table = html_table.replace('<th>', f'<th {th_style}>').replace('<td>', f'<td {td_style}>')
                    # Wrapper com overflow-x para não estourar o e-mail em telas menores
                    tables_html += f'<div style="overflow-x: auto; width: 100%;">{html_table}</div>'
                else:
                    tables_html += "<p style='color: #666;'><strong>Empenhos:</strong> Nenhuma mudança detectada hoje.</p>"
            else:
                tables_html += "<p style='color: #666;'><strong>Empenhos:</strong> Nenhuma mudança detectada.</p>"
        except Exception as e:
            tables_html += f"<p style='color: #cc0000;'><strong>Empenhos:</strong> Erro ao carregar relatório - {e}</p>"
    else:
        if include_empenhos:
            tables_html += "<p style='color: #666;'><strong>Empenhos:</strong> Relatório não encontrado.</p>"

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

# --- Função Principal de Envio de E-mail ---

def send_reports_email():
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT]):
        print("ERRO CRÍTICO: Variáveis de ambiente de e-mail não configuradas. Verifique EMAIL_SENDER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT.")
        return

    recipients_data = get_recipients(EMAILS_FILE)
    if not recipients_data:
        print("ERRO CRÍTICO: Nenhum destinatário válido encontrado. E-mail não será enviado.")
        return

    today_str = datetime.now(tz_brasilia).strftime('%d/%m/%Y')
    report_files = {
        "Execução Orçamentária": os.path.join(BASE_EXEC, "mudancas_execucao.xlsx"),
        "Empenhos": os.path.join(BASE_EXEC, "mudancas_empenhos.xlsx"),
    }
    invalid_reports = []
    valid_report_found = False

    for report_name, report_path in report_files.items():
        if os.path.exists(report_path):
            report_date = get_report_date(report_path)
            if report_date:
                if report_date != today_str:
                    invalid_reports.append((report_name, report_date))
                else:
                    valid_report_found = True
            else:
                invalid_reports.append((report_name, None))

    if invalid_reports:
        messages = []
        for report_name, report_date in invalid_reports:
            if report_date is None:
                messages.append(f"{report_name} sem data válida")
            else:
                messages.append(f"{report_name} com data {report_date}")
        print(f"Atenção: os seguintes relatório(s) não correspondem à data de execução ({today_str}) e serão ignorados: {'; '.join(messages)}")

    attachments_to_send = []
    if os.path.exists(os.path.join(BASE_EXEC, "mudancas_execucao.xlsx")):
        rd = get_report_date(os.path.join(BASE_EXEC, "mudancas_execucao.xlsx"))
        if rd == today_str:
            attachments_to_send.append(os.path.join(BASE_EXEC, "execucao.xlsx"))
            
    if os.path.exists(os.path.join(BASE_EXEC, "mudancas_empenhos.xlsx")):
        rd = get_report_date(os.path.join(BASE_EXEC, "mudancas_empenhos.xlsx"))
        if rd == today_str:
            attachments_to_send.append(os.path.join(BASE_EXEC, "empenhos.xlsx"))

    if not valid_report_found:
        print(f"E-mail não enviado: nenhum relatório com data de execução válida ({today_str}) foi encontrado.")
        return

    include_execucao = os.path.join(BASE_EXEC, "execucao.xlsx") in attachments_to_send
    include_empenhos = os.path.join(BASE_EXEC, "empenhos.xlsx") in attachments_to_send
    tables_content, has_changes = prepare_html_body(BASE_EXEC, include_execucao=include_execucao, include_empenhos=include_empenhos)

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

                # NOVO LAYOUT DE E-MAIL (max-width expandido para 1100px para caber as tabelas com conforto e levemente maiores)
                html_body = f"""
                <html>
                <body style="margin: 0; padding: 0; background-color: #f4f7f6; font-family: 'Segoe UI', Arial, sans-serif; color: #333333;">
                    <div style="max-width: 1100px; margin: 30px auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.08);">
                        
                        <!-- Cabeçalho -->
                        <div style="background-color: #003366; padding: 25px; text-align: center;">
                            <h2 style="margin: 0; color: #ffffff; font-size: 24px; font-weight: normal; letter-spacing: 0.5px;">Relatório de Atualização - SOF</h2>
                        </div>
                        
                        <!-- Corpo Principal -->
                        <div style="padding: 35px;">
                            <p style="font-size: 16px; margin-top: 0;">{saudacao} <strong>{nome}</strong>,</p>
                            
                            <p style="line-height: 1.6; font-size: 15px;">
                                Informamos que <strong>houve mudanças</strong> na execução orçamentária dos contratos de gestão na data de hoje.
                            </p>
                            <p style="line-height: 1.6; font-size: 15px;">
                                <strong>Seguem anexos</strong> os arquivos atualizados contendo a situação detalhada das dotações orçamentárias e dos nossos empenhos. Abaixo, destacamos as alterações detectadas pelo sistema:
                            </p>
                            
                            <!-- Bloco de Tabelas -->
                            <div style="margin-top: 35px; margin-bottom: 35px; width: 100%;">
                                {tables_content}
                            </div>
                            
                            <!-- Rodapé -->
                            <div style="border-top: 1px solid #eeeeee; padding-top: 25px; margin-top: 40px;">
                                <p style="line-height: 1.6; font-size: 14px; color: #666666; margin-bottom: 25px;">
                                    Este é um e-mail automático. Em caso de dúvidas, a equipe está à disposição.
                                </p>
                                <p style="line-height: 1.6; font-size: 15px; margin-bottom: 5px;">Atenciosamente,</p>
                                <img src="cid:signature" style="max-width: 250px; height: auto; display: block; margin-top: 10px;">
                            </div>
                        </div>
                    </div>
                </body>
                </html>
                """
                msg.attach(MIMEText(html_body, 'html', 'utf-8'))

                signature_path = os.path.join(AUX_FILES_PATH, "assinatura.png")
                attach_signature_image(msg, signature_path)

                for attachment_path in attachments_to_send:
                    attach_file(msg, attachment_path)

                server.send_message(msg)
                print(f"E-mail enviado com sucesso para: {email}")

    except smtplib.SMTPAuthenticationError:
        print("ERRO: Falha na autenticação SMTP. Verifique o EMAIL_SENDER e EMAIL_PASSWORD.")
    except smtplib.SMTPConnectError as e:
        print(f"ERRO: Falha ao conectar ao servidor SMTP. Verifique SMTP_SERVER e SMTP_PORT. Detalhes: {e}")
    except Exception as e:
        print(f"ERRO INESPERADO ao enviar e-mail: {e}")

if __name__ == "__main__":
    send_reports_email()