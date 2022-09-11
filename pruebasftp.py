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
#mycol = db["deudors"]
#mycol.drop()
#
#mycol = db["debts"]
#mycol.drop()
#
#mycol = db["debts_custom_fields"]
#mycol.drop()
#
#mycol = db["custom_fields"]
#mycol.drop()
#
#mycol = db["phones"]
#mycol.drop()
#"""

#print(db.list_collection_names())
print("--------------------- DEUDORS ---------------------------")
mycol = db.deudors
#test_range = mycol.find({ "created_at": {"$gte" : datetime(2022, 9, 5), "$lt": datetime(2022, 9, 5)}})
list_db = mycol.find({"organization_id": 17,  "created_at": {"$gte" : datetime(2022, 9, 10)}})  #, "$lt": datetime(2022, 9, 5)}
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
list_db = mycol.find({"organization_id": 14,  "created_at": {"$gte" : datetime(2022, 9, 10)}})  #, "$lt": datetime(2022, 9, 5)}
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
list_db = mycol.find({"organization_id": 14,  "created_at": {"$gte" : datetime(2022, 9, 10)}})  #, "$lt": datetime(2022, 9, 5)}
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
list_db = mycol.find({"organization_id": 14,  "created_at": {"$gte" : datetime(2022, 9, 10)}})  #, "$lt": datetime(2022, 9, 5)}
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
list_db = mycol.find({"organization_id": 14,  "created_at": {"$gte" : datetime(2022, 9, 10)}})  #, "$lt": datetime(2022, 9, 5)}
list = []
list_filtered = []
q=0
for l in list_db:
    q=q+1
    if(q % 50000 == 0):
        print(f'{q} Phones desde db')
print(f'{q} TOTALES Phones desde db')




"""
# Define email sender and receiver
email_sender = 'SOPORTE@MOVATEC.CL'
email_password = 'bphrtpsrkdtqeeat'
email_receiver = 'ivera@movatec.cl'

# Set the subject and body of the email
subject = 'Prueba'
body = """
#Archivos Json Enviados
"""

em = EmailMessage()
em['From'] = email_sender
em['To'] = email_receiver
em['Subject'] = subject
em.set_content(body)

# Add SSL (layer of security)
context = ssl.create_default_context()

# Log in and send the email
with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
    smtp.login(email_sender, email_password)
    smtp.sendmail(email_sender, email_receiver, em.as_string())

return 1
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