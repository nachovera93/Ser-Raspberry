from connector import iot_ser_db
from connector import local_db
import json

db = iot_ser_db()

def insert_data(collection_name, data):
    try:
        col_use = db[collection_name]
        col_use.insert_one(data)
    except Exception as e:
        print(f"Excepción al insertar datos en Mongo: {str(e)}")
        #return -1
    #bf=[]
    #for m in data:
    #    bf.append(m)
    #    print("-------------------------------------")
    #    print(f'Insertando en maximos')
    #    print("-------------------------------------")
    #    print(" ")
    #    print(" ")
    #   
    #col_use.insert_many(data) #.drop_duplicates(subset=['customer_id'], keep='last') #
    #return 1