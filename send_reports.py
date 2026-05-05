import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from consulta_sof import input_with_timeout
import pytz

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
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com") # Padrão para Gmail
SMTP_PORT = int(os.getenv("SMTP_PORT", 587)) # Padrão para TLS

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

def prepare_html_body(base_exec_path):
    """
    Prepara o corpo do e-mail em formato HTML com as tabelas de mudanças.
    """
    has_changes = False
    today_str = datetime.now(tz_brasilia).strftime('%d/%m/%Y')
    tables_html = ""

    # --- Mudanças de Despesas (Execução Orçamentária) ---
    path_mudancas_exec = os.path.join(base_exec_path, "mudancas_execucao.xlsx")
    if os.path.exists(path_mudancas_exec):
        try:
            df_mudancas_exec = pd.read_excel(path_mudancas_exec)
            if not df_mudancas_exec.empty and 'data_hora_extracao' in df_mudancas_exec.columns:
                # Verifica se a data do relatório (armazenada na coluna) é hoje
                report_date = str(df_mudancas_exec['data_hora_extracao'].iloc[0]).split(' ')[0]
                if report_date == today_str:
                    has_changes = True
                    tables_html += "<h3>Mudanças nas Despesas (Execução Orçamentária)</h3>"
                    tables_html += df_mudancas_exec.to_html(index=False, border=1)
                else:
                    tables_html += "<p>Nenhuma mudança detectada hoje nas Despesas (Execução Orçamentária).</p>"
            else:
                tables_html += "<p>Nenhuma mudança detectada nas Despesas (Execução Orçamentária).</p>"
        except Exception as e:
            tables_html += f"<p>Erro ao carregar relatório de mudanças de execução: {e}</p>"
    else:
        tables_html += "<p>Relatório de mudanças de execução não encontrado.</p>"

    # --- Mudanças de Empenhos ---
    path_mudancas_empenhos = os.path.join(base_exec_path, "mudancas_empenhos.xlsx")
    if os.path.exists(path_mudancas_empenhos):
        try:
            df_mudancas_empenhos = pd.read_excel(path_mudancas_empenhos)
            if not df_mudancas_empenhos.empty and 'data_hora_extracao' in df_mudancas_empenhos.columns:
                # Verifica se a data do relatório (armazenada na coluna) é hoje
                report_date = str(df_mudancas_empenhos['data_hora_extracao'].iloc[0]).split(' ')[0]
                if report_date == today_str:
                    has_changes = True
                    tables_html += "<h3>Mudanças nos Empenhos</h3>"
                    tables_html += df_mudancas_empenhos.to_html(index=False, border=1)
                else:
                    tables_html += "<p>Nenhuma mudança detectada hoje nos Empenhos.</p>"
            else:
                tables_html += "<p>Nenhuma mudança detectada nos Empenhos.</p>"
        except Exception as e:
            tables_html += f"<p>Erro ao carregar relatório de mudanças de empenhos: {e}</p>"
    else:
        tables_html += "<p>Relatório de mudanças de empenhos não encontrado.</p>"

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

# --- Função Principal de Envio de E-mail ---

def send_reports_email():
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT]):
        print("ERRO CRÍTICO: Variáveis de ambiente de e-mail não configuradas. Verifique EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT.")
        return

    recipients_data = get_recipients(EMAILS_FILE)
    if not recipients_data:
        print("ERRO CRÍTICO: Nenhum destinatário válido encontrado. E-mail não será enviado.")
        return

    # Verifica mudanças uma única vez antes de iniciar o envio
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
                    <p>Atenciosamente,<br>Nathan Faita</p>
                </body>
                </html>
                """
                msg.attach(MIMEText(html_body, 'html'))

                # Adiciona os anexos
                attach_file(msg, os.path.join(BASE_EXEC, "execucao.xlsx"))
                attach_file(msg, os.path.join(BASE_EXEC, "empenhos.xlsx"))

                server.send_message(msg)
                print(f"E-mail enviado com sucesso para: {email}")

    except smtplib.SMTPAuthenticationError:
        print("ERRO: Falha na autenticação SMTP. Verifique o EMAIL_SENDER e EMAIL_PASSWORD.")
    except smtplib.SMTPConnectError as e:
        print(f"ERRO: Falha ao conectar ao servidor SMTP. Verifique EMAIL_SMTP_SERVER e EMAIL_SMTP_PORT. Detalhes: {e}")
    except Exception as e:
        print(f"ERRO INESPERADO ao enviar e-mail: {e}")

if __name__ == "__main__":
    send_reports_email()
