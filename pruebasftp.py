#from sys import orig_argv
#from multiprocessing.reduction import duplicate
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
dia=22
organization_id=18
"""
print("--------------------- DEUDORS ---------------------------")
mycol = db.deudors
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
#list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)},"campaign_id": "62fbc63e93e165004b362ef0"})  #, "$lt": datetime(2022, 9, 5)}
#list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})
list_db = mycol.find({"organization_id": organization_id})#,  "created_at": {"$gte" : datetime(2022, 9, dia)}})
list = []
list_filtered = []
#c=0
#for z in list_db:
#    c=c+1
#    if(c % 100000 == 0):
#        print(f'{c} DEUDORS desde db')
#print(f'{c} TOTALES DEUDORS desde db')
x=0
for l in list_db:  
        x=x+1
        list.append({'customer_id': l['customer_id'], 'nombre': l['nombre'],'organization_id': l['organization_id']}) #'genero': l['genero'], 'fecha_nacimiento': l['fecha_nacimiento'], 'actividad_profesion': l['actividad_profesion'], 'comuna_part': l['comuna_part'], 'direccion_part': l['direccion_part'], 'region_part': l['region_part'], 'comuna_com': l['comuna_com'], 'direccion_com': l['direccion_com'], 'region_com': l['region_com'], 'codigo_postal': l['codigo_postal']})
        #if(c % 5000 == 0):
        #        print(f'{c} data_db_deudors desde db')
print(f'{x} Totales data_db_deudors desde db')
list_filtered = pd.DataFrame(list).to_dict('records')
df_list_filtered = pd.DataFrame(list_filtered)
duplicados = df_list_filtered[df_list_filtered.duplicated()] 
df_without_duplicates = df_list_filtered.drop_duplicates(subset=['customer_id','nombre','organization_id'],keep=False)  #keep='last'  
df = df_without_duplicates.reset_index(drop=True)
List_duplicates=duplicados.to_dict(orient='records')
print(f'Duplicados en deudors: {x-len(df_without_duplicates)}')   
#Delete_Duplicates=List_duplicates[0:30000]
#print(Delete_Duplicates[0:10])
#dup=0
#for k in Delete_Duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 10000 == 0):
#       print(f'{dup} Borrados Deudors')

##print(d.deleted_count, " documents deleted !!")
"""
print("---------------------------------------------------------") 
print(" ")
print(" ")
print("----------------------- DEBTS ---------------------------")

mycol = db.debts
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}
#list_db = mycol.find({"organization_id": organization_id})
list = []
list_filtered = []
#v=0
#for l in list_db:
#    v=v+1
#    if(v % 50000 == 0):
#        print(f'{v} DEBTS desde db')
#print(f'{v} TOTALES DEBTS desde db')
c=0
for l in list_db:  #creamos nueva lista con datos DESDE mongo y la ingresamos en una list_filtered
        c=c+1
        list.append({'customer_id': l['customer_id'], 'cuenta': l['cuenta'], 'dias_mora': l['dias_mora'], 'deuda_mora': l['deuda_mora'], 'campaign_id': l['campaign_id'], 'deuda_facturada': l['deuda_facturada'], 'deuda_total': l['deuda_total'], 'group_campaign_id': l['group_campaign_id'],'organization_id': l['organization_id']})#list.append({'customer_id': l['customer_id'], 'cuenta': l['cuenta'], 'dias_mora': l['dias_mora'], 'deuda_mora': l['deuda_mora'], 'campaign_id': l['campaign_id'], 'deuda_facturada': l['deuda_facturada'], 'deuda_total': l['deuda_total'], 'group_campaign_id': l['group_campaign_id']})
        #if(c % 5000 == 0):
        #        print(f'{c} data_db_deudors desde db')
print(f'{c} Totales data_db_deudors desde db')
list_filtered = pd.DataFrame(list).to_dict('records')
df_list_filtered = pd.DataFrame(list_filtered)
duplicados = df_list_filtered[df_list_filtered.duplicated()] 
df_without_duplicates = df_list_filtered.drop_duplicates(subset=['customer_id','organization_id','cuenta','dias_mora','campaign_id','deuda_total','group_campaign_id'],keep=False)  #keep='last'  
df = df_without_duplicates.reset_index(drop=True)
List_duplicates=duplicados.to_dict(orient='records')
print(f'Duplicados en deudors: {c-len(df_without_duplicates)}')   
Delete_Duplicates=List_duplicates[0:10]
print(Delete_Duplicates)
#dup=0
#for k in Delete_Duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 10000 == 0):
#       print(f'{dup} Borrados Deudors')



print("---------------------------------------------------------") 
print(" ")
print(" ")
print("--------------------- CUSTOM FIELDS ---------------------")
"""
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
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}})  #, "$lt": datetime(2022, 9, 5)}

#c=0
#for l in list_db:
#    c=c+1
#    mycol.delete_one(l)
#    if(c % 1000 == 0):
#            print(f'{c} data_db_detbs_custom_fileds desde db')
#print(f'{c} totales data_db_detbs_custom_fileds desde db')

#query = {"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}}
#d = mycol.delete_many(query)
#print(d.deleted_count, " documents deleted !!")

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
