import os
import smtplib
from email.message import EmailMessage

def send_email(config, **kwargs):
    ti = kwargs['ti']
    evaluated_data = ti.xcom_pull(task_ids='evaluate_business_logic', key='evaluated_data')
    dag_id = config['dag_id']
    
    email_to = config.get('alert_config', {}).get('email_to', [])
    email_cc = config.get('alert_config', {}).get('email_cc', [])
    
    if not email_to:
        return
        
    alerts = [
        ac for ac in evaluated_data
        if ac['status'] in ['NEW_ENTRY', 'RE_ENTRY_ALERT']
    ]
    
    if not alerts:
        return
        
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    
    msg = EmailMessage()
    msg['Subject'] = f"Aircraft Alert - {dag_id}"
    msg['From'] = smtp_user
    msg['To'] = ", ".join(email_to)
    if email_cc:
        msg['Cc'] = ", ".join(email_cc)
        
    content = "The following aircraft triggered alerts:\n\n"
    for ac in alerts:
        content += f"- Hex: {ac['hex']}, Reg: {ac.get('registration')}, Callsign: {ac.get('callsign')}, Status: {ac['status']}\n"
        
    msg.set_content(content)
    
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
