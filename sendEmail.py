import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Generar un gráfico de barras de ejemplo
def generate_bar_chart():
    objects = ('A', 'B', 'C', 'D', 'E')
    y_pos = np.arange(len(objects))
    performance = [10, 8, 6, 4, 2]

    fig, ax = plt.subplots()
    ax.bar(y_pos, performance, align='center', alpha=0.5)
    ax.set_xticks(y_pos)
    ax.set_xticklabels(objects)
    ax.set_ylabel('Value')
    ax.set_title('Bar Chart Example')

    return fig

# Crear el PDF con el gráfico generado
def create_pdf(filename):
    fig = generate_bar_chart()
    pdf = PdfPages(filename)
    pdf.savefig(fig)
    pdf.close()

# Enviar correo electrónico con archivo adjunto
def send_email(to_email, subject, body, attachment_path):
    from_email = "ricardoignaciovera.93@gmail.com"  # Replace with your email address
    password = "temp1062"  # Replace with your email password
    
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body))

    with open(attachment_path, "rb") as file:
        attach = MIMEApplication(file.read(), _subtype="pdf")
        attach.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment_path))
        msg.attach(attach)

    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(from_email, password)
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()

# Crear y enviar el informe en PDF
def main():
    pdf_filename = "report.pdf"
    create_pdf(pdf_filename)

    to_email = "ricardovera.93@hotmail.com"  # Replace with the recipient's email address
    subject = "Informe mensual"
    body = "Aquí está el informe mensual en PDF."

    send_email(to_email, subject, body, pdf_filename)

if __name__ == "__main__":
    main()