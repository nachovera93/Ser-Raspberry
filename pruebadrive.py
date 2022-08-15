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
datetim=str(datetime.datetime.now()-datetime.timedelta(minutes=3))
#valor = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#valor = datetim.strftime('%H:%M ')
worksheet.update(f'A12',datetim[0:19])
#EAALGJZCNm6ecBAAjKHES85sGlO2sqpNDpleJJ1ZCbw5V6KMNaBLxuifk9tyHl9ZAWrC7nzWZCYq88LRdYNuYG8ZAyw99ZCbtKtrZBOSNyyCU3yubeO6zfOLI9817bLgaA7tE65FjT91CiiK7v475ZC4SkM5t2IgQpFMkiJMqgoXaGgKW8VGHzpo9