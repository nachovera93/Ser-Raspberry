import pymongo
import datetime
from datetime import date, timedelta
from datetime import datetime
#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
import pandas as pd
import json

MONGODB_TIMEOUT = 1000

mydb = pymongo.MongoClient("mongodb://172.16.1.41:27017,172.16.1.42:27017,172.16.1.43:27017/haddacloud-v3?replicaSet=haddacloud-rs")
print("conectando a monngo")
#print(myclient.list_database_names())
empty_df = pd.DataFrame()
print(empty_df)
df_deudors = empty_df.assign(
            customer_id='Hola',   
            nombre='Hola',
            genero=None,
            fecha_nacimiento=None,
            actividad_profesion=None,
            comuna_part='Hola',
            direccion_part='Hola',
            region_part=None,
            comuna_com=None,
            direccion_com=None,
            region_com=None,
            codigo_postal=None )

mydb = mydb["haddacloud-v3"]
mycol = mydb.deudors

collist = mydb.list_collection_names()
if "deudors" in collist:
  print("The collection exists.")

print("---------------")
client="segecob"
organization_id=16
group_campaign_id=84
hola='Hola'
def create_log(client,organization_id,group_campaign_id):
    created = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    col_use  = mydb.deudors
    col_use.insert_one({"client":client,"organization_id":int(organization_id),"group_campaign_id":int(group_campaign_id),"created":created})
    
    return 1
col_use  = mydb.deudors
#col_use.insert_one({"client":client,"organization_id":int(organization_id),"group_campaign_id":int(group_campaign_id)})
mylist = [ {"customer_id":"hola",
                    "nombre":'Hola',
                    "genero":None,
                    "fecha_nacimiento":None,
                    "actividad_profesion":None,
                    "comuna_part":'Hola',
                    "direccion_part":'Hola',
                    "region_part":None,
                    "comuna_com":None,
                    "direccion_com":None,
                    "region_com":None,
                    "codigo_postal":None,
                    "organization_id":16},
                   {"customer_id":hola,
                    "nombre":'Hola',
                    "genero":None,
                    "fecha_nacimiento":None,
                    "actividad_profesion":None,
                    "comuna_part":'Hola',
                    "direccion_part":'Hola',
                    "region_part":None,
                    "comuna_com":None,
                    "direccion_com":None,
                    "region_com":None,
                    "codigo_postal":None,
                    "organization_id":16}
                    ]
col_use.insert_many(mylist)



