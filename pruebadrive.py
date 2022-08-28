
import datetime
from datetime import date
import numpy as np
import json
import openpyxl
import time

datetim=datetime.datetime.now()

vt=time.time()
vt2=time.time()

def variables():
    global vt,vt2
    variable=11
    vt=funcion(vt,variable)
    print(f'vt {vt}')
    vt2=funcion(vt2,variable)
    print(f'vt2 {vt2}')
    
def funcion(vt,variable):
    vttime=time.time()
    if((vttime-vt)>20):
        print("Enviando")
        vt=time.time()
        return vt
    return vt   

def received():
    while True:
        variables()
        time.sleep(1)

if __name__ == '__main__':
    received()
"""
import pymongo
myclient = pymongo.MongoClient("mongodb://devuser:devpassword@54.94.243.121:27017/?authMechanism=DEFAULT")
print(myclient.list_database_names())
""" 
    
#EAALGJZCNm6ecBAAjKHES85sGlO2sqpNDpleJJ1ZCbw5V6KMNaBLxuifk9tyHl9ZAWrC7nzWZCYq88LRdYNuYG8ZAyw99ZCbtKtrZBOSNyyCU3yubeO6zfOLI9817bLgaA7tE65FjT91CiiK7v475ZC4SkM5t2IgQpFMkiJMqgoXaGgKW8VGHzpo9
