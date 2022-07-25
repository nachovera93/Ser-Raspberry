import gspread
import datetime
import numpy as np
import json
gc = gspread.service_account(filename='rep_medidor.json')
print("Paso")
#sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1lCtPvKcNnJqHzQWDFuLZk3g9oeHtigPChP5kjboQ0XU/edit#gid=0')
sh = gc.open('Luis_Wherhahm')
#print(sh.sheet1.get('A1'))
worksheet = sh.worksheet("Hoja 1")
datetim=datetime.datetime.now()-datetime.timedelta(minutes=3)
datetim=json.dumps(datetim.time(), default=str)
#=[f'{datetim.hour}:{datetim.minute}{datetim.minute}']
print(datetime)
worksheet.update('F13',datetim[1:6])


values_list = worksheet.col_values(1)
print(values_list)
print(len(values_list))
OneHourEnergy_RedCompañia=0.1
array = np.array([[round(OneHourEnergy_RedCompañia,5)]])

# Write the array to worksheet starting from the A2 cell
worksheet.update('G13', array.tolist())  
#import numpy as np
#array = np.array([[4, 5, 6]])

# Write the array to worksheet starting from the A2 cell
#worksheet.update('B8', array.tolist())
