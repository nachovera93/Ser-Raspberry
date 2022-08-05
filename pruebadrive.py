import gspread
import datetime
import numpy as np
import json
import openpyxl
gc = gspread.service_account(filename='rep_medidor.json')
sh = gc.open('Luis_Wherhahm')
datetim=datetime.datetime.now()

worksheet = sh.worksheet("Hoja 1")
datetim=datetime.datetime.now()-datetime.timedelta(minutes=3)
datetim=json.dumps(datetim.time(), default=str)
#=[f'{datetim.hour}:{datetim.minute}{datetim.minute}']

#worksheet.update('B13',datetim[1:6])
#worksheet.batch_clear(["A11:A12"])

from openpyxl import Workbook

book = Workbook()
dest_filename = f'Reportes.xlsx'
#sheet1 = book.active
sheet11 = book.create_sheet(f"MaxHora Fase 1 Diario") 
#sheet11 = workbook[f"Var Dispositivos"]
import pprint
lst=[]
Data=[1,2,3,4]
#lst = list(range(5))
lst.append(Data)
print(lst)
#pprint.pprint(lst)
#[0, 1, 2, 3, 4]
#mtx = [list(range(5)) for _ in range(5)]
#sheet11.append(lst)
#print(mtx)
#book.save(filename = dest_filename)
lst.append(Data)
lst.append(Data)
#print(lst)
for i in lst:
    sheet11.append(i)

book.save(filename = dest_filename)

datetim=datetime.datetime.now()-datetime.timedelta(days=1)
datetim=datetime.datetime.now()-datetime.timedelta(minutes=3)
gc = gspread.service_account(filename='rep_medidor.json')
sh = gc.open('Luis_Wherhahm')
worksheet = sh.worksheet("Hoja 1")
values_list = worksheet.col_values(2)
Largo=len(values_list)
val = float(worksheet.acell('T2').value)*1.0
array = np.array([[round(0.0,5)]])
array2 = np.array([[round(0.0,5)]])
array3 = np.array([[round(0.0,5)]]) 
array4 = np.array([[round(val,1)]]) 
#datetim=json.dumps(datetim)
array5 = datetim.date()
print(array5)
array5=np.array(str(array5))
print(array5)
worksheet.update(f'A{Largo+1}',array5.tolist())
worksheet.update(f'B{Largo+1}', array.tolist())
worksheet.update(f'C{Largo+1}', array2.tolist())
worksheet.update(f'D{Largo+1}', array3.tolist())
worksheet.update(f'E{Largo+1}', array4.tolist())
values_list = worksheet.col_values(6)
Largo=len(values_list)



#EAALGJZCNm6ecBAAjKHES85sGlO2sqpNDpleJJ1ZCbw5V6KMNaBLxuifk9tyHl9ZAWrC7nzWZCYq88LRdYNuYG8ZAyw99ZCbtKtrZBOSNyyCU3yubeO6zfOLI9817bLgaA7tE65FjT91CiiK7v475ZC4SkM5t2IgQpFMkiJMqgoXaGgKW8VGHzpo9