import gspread

gc = gspread.service_account(filename='rep_medidor.json')
print("Paso")
#sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1lCtPvKcNnJqHzQWDFuLZk3g9oeHtigPChP5kjboQ0XU/edit#gid=0')
sh = gc.open('Luis_Wherhahm')
print(sh.sheet1.get('A1'))
worksheet = sh.worksheet("Hoja 1")
worksheet.update('B20', 'Bingo!')



import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
try:
    s.connect(("www.google.com", 80))
except (socket.gaierror, socket.timeout):
    print("Sin conexión a internet")
else:
    print("Con conexión a internet")
    s.close()

import numpy as np
array = np.array([[4, 5, 6]])

# Write the array to worksheet starting from the A2 cell
worksheet.update('B8', array.tolist())
