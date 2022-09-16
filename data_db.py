from connector import iot_ser_db
from connector import local_db

db = iot_ser_db()

def list_data_db_insert(data):
    try:
        print(data)
        col_use = db.historic_data
        col_use.insert_many(data)
        #list_db = col_use.find({"userId": userId})
    except Exception as e:
        print(f"Excepcion en Insertar datos a mongo: {str(e)} ")
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