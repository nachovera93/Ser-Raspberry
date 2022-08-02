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




val = float(worksheet.acell('B2').value)
print(val)
#yast lan
#vicibox10:~ # install-vicibox