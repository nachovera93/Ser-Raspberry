import datetime
import json
import os

import openpyxl
import pandas as pd
import pymongo

from connector import iot_ser_db, local_db

db = iot_ser_db()
col_use = db.monthmaxs


query = {"userId": "62f3d5563f5269001b12058a"}
new_values = {"$set": {"userId": "629141a62052def848c8801b"}}

#update the document
col_use.update_many(query, new_values)

# access all the documents
#for i in collection.find():
#    print(i)
