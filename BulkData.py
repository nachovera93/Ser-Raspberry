import time
import datetime
from connector import iot_ser_db
from connector import local_db
import json

DataBase = iot_ser_db()


def list_data_db_insert(data):
    try:
        collection_use = DataBase.historic_data
        collection_use.insert_one(data)
    except Exception as e:
        print(f"Excepcion en Insertar datos a mongo: {str(e)} ")


def data_db_insert():
    try:
        collection_use = DataBase.devices
        DataFromDB = [x for x in collection_use.find_one({"selected": False, "name":"Medidor"})]
        print(DataFromDB)
        print(len(DataFromDB))
    except Exception as e:
        print(f"Excepcion en Insertar datos a mongo: {str(e)} ")

 # $lt: ISODate("2022-09-20")


def ConsultTypeOfData():
    try:
        collection_use = DataBase.historic_data
        DataFromDB = [x for x in collection_use.find({
            'userId': {
                '$exists': False
            }
        })]
        print(len(DataFromDB))
    except Exception as e:
        print(f"Excepcion en Insertar datos a mongo: {str(e)} ")


def UpdateDatainDatabase():
    collection_use = DataBase.historic_data
    result = collection_use.update_many({
        "userId": {
            "$exists": False
        }
    },
        {
        "$set": {
            "userId": "62f3d5563f5269001b12058a"
        }

    }
    )
    print("Data updated with id", result)


data_db_insert()
#ConsultTypeOfData()
#UpdateDatainDatabase()

s = "01-12-2011 21:17:04"
print(time.mktime(datetime.datetime.strptime(
    s, "%d-%m-%Y %H:%M:%S").timetuple()))  # %H:%M:%S"
