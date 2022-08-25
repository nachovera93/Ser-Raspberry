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


mydbs = mydb["haddacloud-v3"]
mycol = mydbs.custom_fields 
mycol.drop()

mycol = mydbs.debts_custom_fields
mycol.drop()

mycol = mydbs.deudors   
mycol.drop()

mycol = mydbs.debts
mycol.drop()

mycol = mydbs.phones
mycol.drop()
"""
import pandas as pd

df_with_duplicates = pd.DataFrame({
    'id': [302, 504, 708, 103, 303, 302],
    'Name': ['Watch', 'Camera', 'Phone', 'Shoes', 'Watch', 'Watch'],
    'Cost': ["300", "400", "350", "100", "300", "300"]
})

#df_without_duplicates = df_with_duplicates.drop_duplicates(keep=False)


df_without_duplicates=df_with_duplicates.drop_duplicates(subset=['Name'],keep=False)  

print("DataFrame with duplicates:")
print(df_with_duplicates, "\n")

print("DataFrame without duplicates:")
print(df_without_duplicates, "\n")
"""