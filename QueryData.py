#from sys import orig_argv
#from multiprocessing.reduction import duplicate
import psycopg2
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
import gc
from unicodedata import normalize

MONGODB_TIMEOUT = 1000

mydb = pymongo.MongoClient("mongodb://172.16.1.41:27017,172.16.1.42:27017,172.16.1.43:27017/haddacloud-v2?replicaSet=haddacloud-rs")
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
##""""

#print(db.list_collection_names())
dia=5
organization_id=14
campaign_id="62eaa70d93e165004b34cfd9"
#{organization_id:18, customer_id:"10013359"}

print("--------------------- DEUDORS ---------------------------")
mycol = db.deudors
##test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
##list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)},"campaign_id": "631642b325c4ea004b556d09"})  #, "$lt": datetime(2022, 9, 5)}
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 10, dia)}})
#list_db = mycol.find({"organization_id": organization_id})#,  "created_at": {"$gte" : datetime(2022, 9, dia)}})
list = []
list_filtered = []
c=0
for l in list_db:
    c=c+1
    if(c % 10000 == 0):
        print(f'{c} DEUDORS desde db')
print(f'{c} TOTALES DEUDORS desde db')
#x=0
#for l in list_db:  
#        x=x+1
#        list.append({'customer_id': l['customer_id'],'organization_id': l['organization_id']}) #'genero': l['genero'], 'fecha_nacimiento': l['fecha_nacimiento'], 'actividad_profesion': l['actividad_profesion'], 'comuna_part': l['comuna_part'], 'direccion_part': l['direccion_part'], 'region_part': l['region_part'], 'comuna_com': l['comuna_com'], 'direccion_com': l['direccion_com'], 'region_com': l['region_com'], 'codigo_postal': l['codigo_postal']})
#        if(x % 5000 == 0):
#                print(f'{x} data_db_deudors desde db')
#print(f'{x} Totales data_db_deudors desde db')
#list_filtered = pd.DataFrame(list).to_dict('records')
#df_list_filtered = pd.DataFrame(list_filtered)
#duplicados = df_list_filtered[df_list_filtered.duplicated()] 
#df_without_duplicates = df_list_filtered.drop_duplicates(subset=['customer_id','organization_id'],keep=False)  #keep='last'  
#df = df_without_duplicates.reset_index(drop=True)
#List_duplicates=duplicados.to_dict(orient='records')
#print(f"Cantidad de df_without_duplicates: {len(df_without_duplicates)}")
#print(f"Cantidad de duplicados: {len(List_duplicates)}")
#print(f'Duplicados en deudors: {x-len(df_without_duplicates)}')   
##Delete_Duplicates=List_duplicates
##print(Delete_Duplicates[0:10])
#dup=0
#for k in List_duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 1000 == 0):
#       print(f'{dup} Borrados Deudors')
#del List_duplicates
#del df_without_duplicates
#gc.collect()
##print(d.deleted_count, " documents deleted !!")

print("---------------------------------------------------------") 
print(" ")
print(" ")
print("----------------------- DEBTS ---------------------------")

mycol = db.debts
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 10, dia)}})  #, "$lt": datetime(2022, 9, 5)}
#list_db = mycol.find({"organization_id": organization_id}) 
#list_db = mycol.find({"organization_id": organization_id,"campaign_id": campaign_id})
list = []
list_filtered = []
v=0
for l in list_db:
    v=v+1
    if(v % 50000 == 0):
        print(f'{v} DEBTS desde db')
print(f'{v} TOTALES DEBTS desde db')
#c=0
#for l in list_db:  #creamos nueva lista con datos DESDE mongo y la ingresamos en una list_filtered
#        c=c+1
#        list.append({'customer_id': l['customer_id'], 'cuenta': l['cuenta'], 'dias_mora': l['dias_mora'], 'deuda_mora': l['deuda_mora'], 'campaign_id': l['campaign_id'], 'deuda_facturada': l['deuda_facturada'], 'deuda_total': l['deuda_total'], 'group_campaign_id': l['group_campaign_id'],'organization_id': l['organization_id']})#list.append({'customer_id': l['customer_id'], 'cuenta': l['cuenta'], 'dias_mora': l['dias_mora'], 'deuda_mora': l['deuda_mora'], 'campaign_id': l['campaign_id'], 'deuda_facturada': l['deuda_facturada'], 'deuda_total': l['deuda_total'], 'group_campaign_id': l['group_campaign_id']})
#        if(c % 5000 == 0):
#                print(f'{c} data_db_deudors desde db')
#print(f'{c} Totales data_db_deudors desde db')
#list_filtered = pd.DataFrame(list).to_dict('records')
#df_list_filtered = pd.DataFrame(list_filtered)
#duplicados = df_list_filtered[df_list_filtered.duplicated()] 
#df_without_duplicates = df_list_filtered.drop_duplicates(subset=['customer_id','organization_id','cuenta','dias_mora','campaign_id','deuda_total','group_campaign_id'],keep=False)  #keep='last'  
#df = df_without_duplicates.reset_index(drop=True)
#List_duplicates=duplicados.to_dict(orient='records')
#print(f"Cantidad de df_without_duplicates: {len(df_without_duplicates)}")
#print(f"Cantidad de duplicados: {len(List_duplicates)}")
#print(f'Duplicados en debts: {c-len(df_without_duplicates)}')   
##Delete_Duplicates=List_duplicates
##print(Delete_Duplicates)
##query = {"organization_id": organization_id,  "created_at": {"$lte" : datetime(2022, 9, dia)}, "campaign_id": campaign_id}
###query = {"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}}
##d = mycol.delete_many(query)
##print(d.deleted_count, " documents deleted !!")
#dup=0
#for k in List_duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 1000 == 0):
#       print(f'{dup} Borrados Deudors')
#print(f'{dup} Borrados Deudors')
#del List_duplicates
#del df_without_duplicates
#gc.collect()

print("---------------------------------------------------------") 
print(" ")
print(" ")
print("--------------------- CUSTOM FIELDS ---------------------")

mycol = db.custom_fields
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 10, dia)}})  #, "$lt": datetime(2022, 9, 5)}
#list_db = mycol.find({"organization_id": organization_id})
list = []
list_filtered = []
b=0
for l in list_db:
    b=b+1
    if(b % 50000 == 0):
        print(f'{b} CUSTOM_FIELDS desde db')
print(f'{b} TOTALES CUSTOM_FIELDS desde db')

#c = 0
#for l in list_db:
#    c = c+1
#    list.append({'debtor_id': l['debtor_id'],'organization_id': l['organization_id'],'group_campaign_id': l['group_campaign_id'], 'customfield1label': l['customfield1label'], 'customfield1': l['customfield1'], 'customfield2label': l['customfield2label'], 'customfield2': l['customfield2'], 'customfield3label': l['customfield3label'], 'customfield3': l['customfield3'], 'customfield4label': l['customfield4label'], 'customfield4': l['customfield4'], 'customfield5label': l['customfield5label'], 'customfield5': l['customfield5'], 'customfield6label': l['customfield6label'], 'customfield6': l['customfield6'], 'customfield7label': l['customfield7label'], 'customfield7': l['customfield7'], 'customfield8label': l['customfield8label'], 'customfield8': l['customfield8'], 'customfield9label': l['customfield9label'], 'customfield9': l['customfield9'], 'customfield10label': l['customfield10label'], 'customfield10': l['customfield10']})
#    if(c % 50000 == 0):
#            print(f'{c} data_db_custom desde db')
#print(f'{c} totales desde data_db_custom desde db')
#
#
#list_filtered = pd.DataFrame(list).to_dict('records')
#df_list_filtered = pd.DataFrame(list_filtered)
#duplicados = df_list_filtered[df_list_filtered.duplicated()] 
#df_without_duplicates = df_list_filtered.drop_duplicates(subset=['debtor_id','organization_id', 'customfield1label', 'customfield1', 'customfield2label', 'customfield2', 'customfield3label', 'customfield3', 'customfield4label', 'customfield4', 'customfield5label',
#                                                  'customfield5', 'customfield6label', 'customfield6', 'customfield7label', 'customfield7', 'customfield8label', 'customfield8', 'customfield9label', 'customfield9', 'customfield10label', 'customfield10', 'group_campaign_id'],keep=False)  #keep='last'  
#df = df_without_duplicates.reset_index(drop=True)
#List_duplicates=duplicados.to_dict(orient='records')
#print(f"Cantidad de df_without_duplicates: {len(df_without_duplicates)}")
#print(f"Cantidad de duplicados: {len(List_duplicates)}")
#print(f'Duplicados en custom fields: {c-len(df_without_duplicates)}')  
#dup=0
#for k in List_duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 10000 == 0):
#       print(f'{dup} Borrados custom')
#
#del List_duplicates
#del df_without_duplicates
#gc.collect()
#
print("---------------------------------------------------------") 
print(" ")
print(" ")
print("------------------ debts_custom_fields ------------------")

mycol = db.debts_custom_fields
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 10, dia)}})  #, "$lt": datetime(2022, 9, 5)}
#list_db = mycol.find({"organization_id": organization_id})
c=0
for l in list_db:
    c=c+1
    mycol.delete_one(l)
    if(c % 1000 == 0):
            print(f'{c} data_db_detbs_custom_fileds desde db')
print(f'{c} totales data_db_detbs_custom_fileds desde db')

#query = {"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 9, dia)}}
#d = mycol.delete_many(query)
#print(d.deleted_count, " documents deleted !!")

#list = []
#list_filtered = []
#a=0
#for l in list_db:
#    a=a+1
#    if(a % 50000 == 0):
#        print(f'{a} debts_custom_fields desde db')
#print(f'{a} TOTALES debts_custom_fields desde db')

#c=0
#for l in list_db:
#    c=c+1
#    list.append({'customer_id': l['customer_id'],'organization_id': l['organization_id'],'customfield1label': l['customfield1label'], 'customfield1': l['customfield1'], 'customfield2label': l['customfield2label'], 'customfield2': l['customfield2'], 'customfield3label': l['customfield3label'], 'customfield3': l['customfield3'], 'customfield4label': l['customfield4label'], 'customfield4': l['customfield4'], 'customfield5label': l['customfield5label'], 'customfield5': l['customfield5'], 'customfield6label': l['customfield6label'], 'customfield6': l['customfield6'], 'customfield7label': l['customfield7label'], 'customfield7': l['customfield7'], 'customfield8label': l['customfield8label'], 'customfield8': l['customfield8'], 'customfield9label': l['customfield9label'], 'customfield9': l['customfield9'], 'customfield10label': l['customfield10label'], 'customfield10': l['customfield10']})
#    if(c % 50000 == 0):
#            print(f'{c} data_db_detbs_custom_fileds desde db')
#print(f'{c} totales data_db_detbs_custom_fileds desde db')
#
#
#list_filtered = pd.DataFrame(list).to_dict('records')
#df_list_filtered = pd.DataFrame(list_filtered)
#duplicados = df_list_filtered[df_list_filtered.duplicated()] 
#df_without_duplicates = df_list_filtered.drop_duplicates(subset=['customer_id','organization_id', 'customfield1label', 'customfield1', 'customfield2label', 'customfield2', 'customfield3label', 'customfield3', 'customfield4label', 'customfield4', 'customfield5label',
#                                                      'customfield5', 'customfield6label', 'customfield6', 'customfield7label', 'customfield7', 'customfield8label', 'customfield8', 'customfield9label', 'customfield9', 'customfield10label', 'customfield10' ],keep=False)      
#df = df_without_duplicates.reset_index(drop=True)
#List_duplicates=duplicados.to_dict(orient='records')
#print(f"Cantidad de df_without_duplicates: {len(df_without_duplicates)}")
#print(f"Cantidad de duplicados: {len(List_duplicates)}")
#print(f'Duplicados en custom fields: {c-len(df_without_duplicates)}')  
#dup=0
#for k in List_duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 10000 == 0):
#       print(f'{dup} Borrados custom')
#
#del List_duplicates
#del df_without_duplicates
#gc.collect()
#
print("---------------------------------------------------------") 
print(" ")
print(" ")
print("--------------------- Phones ---------------------")
mycol = db.phones
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": organization_id,  "created_at": {"$gte" : datetime(2022, 10, dia)}})  #, "$lt": datetime(2022, 9, 5)}
#list_db = mycol.find({"organization_id": organization_id})
list = []
list_filtered = []
q=0
for l in list_db:
    q=q+1
    if(q % 50000 == 0):
        print(f'{q} Phones desde db')
print(f'{q} TOTALES Phones desde db')

#c=0
#for l in list_db:  #creamos nueva lista con datos DESDE mongo y la ingresamos en una list_filtered
#        c=c+1
#        list.append({'organization_id': l['organization_id'], 'phone': l['phone'], 'debtor_id': l['debtor_id']})
#        #if(c % 5000 == 0):
#        #        print(f'{c} data_db_deudors desde db')
#print(f'{c} Totales phones desde db')
#list_filtered = pd.DataFrame(list).to_dict('records')
#df_list_filtered = pd.DataFrame(list_filtered)
#duplicados = df_list_filtered[df_list_filtered.duplicated()] 
#df_without_duplicates = df_list_filtered.drop_duplicates(subset=['phone','organization_id','debtor_id'],keep=False)  #keep='last'  
#df = df_without_duplicates.reset_index(drop=True)
#List_duplicates=duplicados.to_dict(orient='records')
#print(f"Cantidad de df_without_duplicates: {len(df_without_duplicates)}")
#print(f"Cantidad de duplicados: {len(List_duplicates)}")
#print(f'Duplicados en custom fields: {c-len(df_without_duplicates)}')  
#dup=0
#for k in List_duplicates:
#    dup=dup+1   
#    #print(f'{k}')
#    query = k
#    d = mycol.delete_one(query)
#    if(dup % 10000 == 0):
#       print(f'{dup} Borrados custom')
#
#del List_duplicates
#del df_without_duplicates
#gc.collect()

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
      



