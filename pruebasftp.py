#from sys import orig_argv
from matplotlib import test
import pymongo
import datetime
from datetime import date, timedelta
from datetime import datetime
#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
import pandas as pd
import json
import os
import re
from unicodedata import normalize

MONGODB_TIMEOUT = 1000

mydb = pymongo.MongoClient("mongodb://172.16.1.41:27017,172.16.1.42:27017,172.16.1.43:27017/haddacloud-v3?replicaSet=haddacloud-rs")
print("conectando a monngo")
#print(myclient.list_database_names())

db = mydb["haddacloud-v2"]

#"""
##mycol = db["deudors"]
##mycol.drop()
##
##mycol = db["debts"]
##mycol.drop()
##
##mycol = db["debts_custom_fields"]
##mycol.drop()
##
##mycol = db["custom_fields"]
##mycol.drop()
##
##mycol = db["phones"]
##mycol.drop()
#"""

#print(db.list_collection_names())
p=0
dia=15
organization_id=18
print("--------------------- DEUDORS ---------------------------")
mycol = db.deudors
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}
list = []
list_filtered = []
c=0
for l in list_db:
    c=c+1
    if(c % 100000 == 0):
        print(f'{c} DEUDORS desde db')
print(f'{c} TOTALES DEUDORS desde db')

print("---------------------------------------------------------") 
print(" ")
print(" ")
print("----------------------- DEBTS ---------------------------")

mycol = db.debts
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}
list = []
list_filtered = []
v=0
for l in list_db:
    v=v+1
    if(v % 50000 == 0):
        print(f'{v} DEBTS desde db')
print(f'{v} TOTALES DEBTS desde db')


print("---------------------------------------------------------") 
print(" ")
print(" ")
print("--------------------- CUSTOM FIELDS ---------------------")

mycol = db.custom_fields
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}
list = []
list_filtered = []
b=0
for l in list_db:
    b=b+1
    if(b % 50000 == 0):
        print(f'{b} CUSTOM_FIELDS desde db')
print(f'{b} TOTALES CUSTOM_FIELDS desde db')

print("---------------------------------------------------------") 
print(" ")
print(" ")
print("------------------ debts_custom_fields ------------------")

mycol = db.debts_custom_fields
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}
list = []
list_filtered = []
a=0
for l in list_db:
    a=a+1
    if(a % 50000 == 0):
        print(f'{a} debts_custom_fields desde db')
print(f'{a} TOTALES debts_custom_fields desde db')


print("---------------------------------------------------------") 
print(" ")
print(" ")
print("--------------------- Phones ---------------------")

mycol = db.phones
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}
list = []
list_filtered = []
q=0
for l in list_db:
    q=q+1
    if(q % 50000 == 0):
        print(f'{q} Phones desde db')
print(f'{q} TOTALES Phones desde db')

if(p==1):
      try:
          # Define email sender and receiver
              from email.message import EmailMessage
              import smtplib
              dia=date.today()
              remitente = "bulk@haddacloud.com"
              destinatario = "ricardovera.93@hotmail.com"
              destinatario2 = "jlvargas@movatec.cl"
              #mensaje = (f'{c} DEUDORS desde db organization_id:\n{v} DEBTS desde db organization_id:\n{b} CUSTOM_FIELDS desde db organization_id:\n{a} debts_custom_fields desde db organization_id:\n{q} Phones desde db organization_id:\n')
              mensaje = (f'Reporte carga archivo organización: {organization_id}\nFilename:{filename}\n Dia: {dia}')
              email = EmailMessage()
              email["From"] = remitente
              email["To"] = destinatario
              email["Subject"] = f'Reporte carga archivo {organization_id} {dia}'
              email.set_content(mensaje)
              
              with open(f'{dia}.txt', "rb") as f:
                  email.add_attachment(
                      f.read(),
                      filename=f'{dia}.txt',
                      maintype="application",
                      subtype="txt"
                  )
              
              smtp = smtplib.SMTP_SSL("mail.haddacloud.com")
              # O si se usa TLS:
              # smtp = SMTP("smtp.ejemplo.com", port=587)
              # smtp.starttls()
              smtp.login(remitente, ",xeQTS3zpRoj")
              smtp.sendmail(remitente, destinatario, email.as_string())
              smtp.sendmail(remitente, destinatario2, email.as_string())
              smtp.quit()
              with open(f'{dia}.txt', 'a') as f: 
                  f.write("\n ")
                  f.write("\n ")
                  f.write("\n ")
                  f.write("\n ")
                  f.write("------------------------------------------------\n")
                  f.write(f'Mail enviado\n')
                  f.write("-------------------------------------------------\n") 
      except Exception as e:
              
              print(f"No pudo hacer envio de email {str(e)} ")  
              with open(f'{dia}.txt', 'a') as f: 
                  f.write("\n ")
                  f.write("\n ")
                  f.write("\n ")
                  f.write("\n ")
                  f.write("------------------------------------------------\n")
                  f.write(f'No pudo hacer envio de email\n')
                  f.write("-------------------------------------------------\n") 
      
      
"""


from email.message import EmailMessage
import smtplib
remitente = "bulk@haddacloud.com"
destinatario = "ricardovera.93@hotmail.com"
#mensaje = (f'{c} DEUDORS desde db organization_id:\n{v} DEBTS desde db organization_id:\n{b} CUSTOM_FIELDS desde db organization_id:\n{a} debts_custom_fields desde db organization_id:\n{q} Phones desde db organization_id:\n')
mensaje = "hola" 
email = EmailMessage()
email["From"] = remitente
email["To"] = destinatario
email["Subject"] = "Bulk SBPAY"
email.set_content(mensaje)

with open("mi_fichero.txt", "rb") as f:
    email.add_attachment(
        f.read(),
        filename="mi_fichero.txt",
        maintype="application",
        subtype="txt"
    )

smtp = smtplib.SMTP_SSL("mail.haddacloud.com")
# O si se usa TLS:
# smtp = SMTP("smtp.ejemplo.com", port=587)
# smtp.starttls()
smtp.login(remitente, ",xeQTS3zpRoj")
smtp.sendmail(remitente, destinatario, email.as_string())
smtp.quit()
"""

#pd_read_csv('ABCDIN_TARJ_GBI_20220501 _OKEY.csv')
"""
# 1. Open the CSV file in reading mode and the TXT file in writing mode
with open('ABCDIN_TARJ_GBI_20220501.csv', 'r') as f_in, open('my_file.txt', 'w') as f_out:
    # 2. Read the CSV file and store in variable
    content = f_in.read()
    # 3. Write the content into the TXT file
    f_out.write(content)
"""
