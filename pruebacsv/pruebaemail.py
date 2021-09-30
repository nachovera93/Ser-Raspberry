import smtplib, ssl
import getpass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import datetime

Lugar="Santa Cristina"
username = "empresasserspa@gmail.com"
password = "empresasserspa"
destinatario = "empresasserspa@gmail.com"
destinatario2 = "demetrio.vera@serm.cl"


mensaje = MIMEMultipart("Alternative")
mensaje["Subject"] = "Reportes "+str(Lugar)+" "+str(datetime.date.today())
mensaje["From"] = username
mensaje["To"] = destinatario

html = f"""
<html>
<body>
     <p> Hola <i>{destinatario}</i> <br>
     Reportes desde {Lugar} </b>
</body>
</html>
"""

parte_html = MIMEText(html, "html")
mensaje.attach(parte_html)

archivo = "prueba_Medidor.xlsx"

with open(archivo, "rb") as adjunto:
     contenido_adjunto = MIMEBase("application", "octet-stream")
     contenido_adjunto.set_payload(adjunto.read())

encoders.encode_base64(contenido_adjunto)

contenido_adjunto.add_header(
     "Content-Disposition",
     f"attachment; filename= {archivo}",
)

mensaje.attach(contenido_adjunto)
text = mensaje.as_string()


context = ssl.create_default_context()

with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
      server.login(username,password)
      print("Sesión Iniciada Correctamente !")
      #server.sendmail(username, destinatario, mensaje)
      server.sendmail(username, destinatario, text)
      server.sendmail(username, destinatario2, text)
      print("Mensaje Enviado Correctamente !")
