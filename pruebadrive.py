import gspread
import datetime
import numpy as np
gc = gspread.service_account(filename='rep_medidor.json')
print("Paso")
#sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1lCtPvKcNnJqHzQWDFuLZk3g9oeHtigPChP5kjboQ0XU/edit#gid=0')
sh = gc.open('Luis_Wherhahm')
#print(sh.sheet1.get('A1'))
worksheet = sh.worksheet("Hoja 1")
datetim=datetime.datetime.now()-datetime.timedelta(minutes=3)
dataHour=[f'{datetim.hour}:{datetim.minute}{datetim.minute}']
worksheet.update('E12',f'{datetim.hour}:{datetim.minute}{datetim.minute}')


values_list = worksheet.col_values(1)
print(values_list)
print(len(values_list))
#import numpy as np
#array = np.array([[4, 5, 6]])

# Write the array to worksheet starting from the A2 cell
#worksheet.update('B8', array.tolist())
