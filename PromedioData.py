from connector import iot_ser_db
from connector import local_db
import json
import pymongo
import datetime
import os
import openpyxl
import pandas as pd

userId="62f3d5563f5269001b12058a"
dId=1212
db = iot_ser_db()

col_use = db.monthmaxs

pipeline = [
    {
        "$group": {
            "_id": "$userId",
            "promedio": {"$avg": "$energia_fase_1_redcompañia_mensual"}
        }
    }
]

result = col_use.aggregate(pipeline)
#print(result)
#El resultado es un objeto CommandCursor, que es un iterador que puede 
#utilizarse para recorrer los resultados de la agregación. Para obtener el 
#promedio, puede recorrer el iterador y acceder al campo promedio del primer elemento:
#Recorremos el cursor para imprimir los resultados
for doc in result:
    print(doc)
from statistics import mean

#mean=[]
#for r in result:
#    if(r['promedio']!=None):
#        print(r)
#        mean.append(r['promedio'])
#
#mean = [r['promedio'] for r in result if r['promedio'] is not None]
#
#average = sum(mean) / len(mean)
#print(average)


