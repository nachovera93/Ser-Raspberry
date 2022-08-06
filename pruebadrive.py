import gspread
import datetime
from datetime import date
import numpy as np
import json
import openpyxl

datetim=datetime.datetime.now()
print(type(datetim.hour))

from openpyxl import Workbook

gc = gspread.service_account(filename='rep_medidor.json')
sh = gc.open('Luis_Wherhahm')
worksheet = sh.worksheet("Hoja 1")
values_list = worksheet.col_values(32)
print(values_list)
Largo=len(values_list)
print(Largo)
array = np.array([[round(113,5)]])
datetim=datetime.datetime.now()
if(datetim.minute<20 and datetim.minute>4):
    minute=15
elif(datetim.minute>0 and datetim.minute<4 ):
    minute=0
elif(datetim.minute>20 and datetim.minute<34):
    minute=30
elif(datetim.minute<50 and datetim.minute>34):
    minute=45
i=1
if(i==1):
    array5=np.array(f'{datetim.hour}:{minute}')
    worksheet.update(f'AF{Largo+1}',array5.tolist())
    worksheet.update(f'AG{Largo+1}',array.tolist())

#EAALGJZCNm6ecBAAjKHES85sGlO2sqpNDpleJJ1ZCbw5V6KMNaBLxuifk9tyHl9ZAWrC7nzWZCYq88LRdYNuYG8ZAyw99ZCbtKtrZBOSNyyCU3yubeO6zfOLI9817bLgaA7tE65FjT91CiiK7v475ZC4SkM5t2IgQpFMkiJMqgoXaGgKW8VGHzpo9