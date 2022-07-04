from openpyxl import Workbook
from datetime import date, timedelta
from datetime import datetime
from openpyxl.styles import Alignment  
from openpyxl.chart import (
    PieChart,
    BarChart,
    ProjectedPieChart,
    Reference
)
import datetime
import openpyxl
from openpyxl.chart.series import DataPoint
import os
import numpy as np


workbook=openpyxl.load_workbook(filename = '2022-06-16.xlsx')
sheet1 = workbook[f"REDCompañia-Fase-1"]
sheet2 = workbook[f"REDCompañia-Fase-2"]
sheet3 = workbook[f"REDCompañia-Fase-3"]
sheet4 = workbook[f"CentralFotovoltaica-Fase-1"]
sheet5 = workbook[f"CentralFotovoltaica-Fase-2"]
sheet6 = workbook[f"CentralFotovoltaica-Fase-3"]
sheet7 = workbook[f"ConsumoCliente-Fase-1"]
sheet8 = workbook[f"ConsumoCliente-Fase-2"]
sheet9 = workbook[f"ConsumoCliente-Fase-3"]
LargeSheet11=len(sheet1["FP"])
LargeSheet21=len(sheet2["FP"])
LargeSheet31=len(sheet3["FP"])
LargeSheet41=len(sheet4["FP"])
LargeSheet51=len(sheet5["FP"])
LargeSheet61=len(sheet6["FP"])
LargeSheet71=len(sheet7["FP"])
LargeSheet81=len(sheet8["FP"])
LargeSheet91=len(sheet9["FP"])
Energy_1 = float(sheet1[f'm{LargeSheet11}'].value)
Energy_2 = float(sheet2[f'm{LargeSheet21}'].value)
Energy_3 = float(sheet3[f'm{LargeSheet31}'].value)
Energy_4 = float(sheet4[f'm{LargeSheet41}'].value)
Energy_5 = float(sheet5[f'm{LargeSheet51}'].value)
Energy_6 = float(sheet6[f'm{LargeSheet61}'].value)
Energy_7 = float(sheet7[f'm{LargeSheet71}'].value)
Energy_8 = float(sheet8[f'm{LargeSheet81}'].value)
Energy_9 = float(sheet9[f'm{LargeSheet91}'].value)

Pos=len(sheet1['A'])


headings0 = list(['Energia1','Energia2','Energia3'])
headings1 = list(['Energia4','Energia5','Energia6'])
headings2 = list(['Energia7','Energia8','Energia9'])
ceros=list([0,0,0,0,0,0,0,0,0])
unos=list([round(Energy_1,2),round(Energy_2,2),round(Energy_3,2),round(Energy_4,2),round(Energy_5,2),round(Energy_6,2),round(Energy_7,2),round(Energy_8,2),round(Energy_9,2)])
datetim=datetime.datetime.now()-datetime.timedelta(minutes=5)

book = Workbook()
sheet22  = book.create_sheet("Sheet1")
sheet22.append(headings0)
sheet23  = book.create_sheet("Sheet2")
sheet23.append(headings1)
sheet24  = book.create_sheet("Sheet3")
sheet24.append(headings2)
book.save(filename = 'pie.xlsx')


workbook2=openpyxl.load_workbook(filename = 'pie.xlsx')
sheet22 = workbook2[f"Sheet1"]
sheet23 = workbook2[f"Sheet2"]
sheet24 = workbook2[f"Sheet3"]

DataHourFase1=[f'{datetim.day}-{datetim.month}-{datetim.year}',round(Energy_1,5),round(Energy_2,5),round(Energy_3,5)]
DataHourFase2=[f'{datetim.day}-{datetim.month}-{datetim.year}',round(Energy_4,5),round(Energy_5,5),round(Energy_6,5)]
DataHourFase3=[f'{datetim.day}-{datetim.month}-{datetim.year}',round(Energy_7,5),round(Energy_8,5),round(Energy_9,5)]

sheet22.append(list(DataHourFase1))
sheet23.append(list(DataHourFase2))
sheet24.append(list(DataHourFase3))


today = date.today()
print("El mes actual es {}".format(today.month))
mes=today.month
dia=date.today()-timedelta(1)
dia=str(dia)
print(dia)

ExcelDia=[]
with open('mi_fichero.txt', 'w') as f:
        horaDesconexión=datetime.datetime.now()
        f.write(f'Reinicio por desconexión: {horaDesconexión}')
#sheet22.delete_rows(11)
ejemplo_dir = '/Users/ignaciovera/Desktop/Codigos/SER-Raspberry/Ser-Raspberry/'
x=0
for f in os.listdir('/Users/ignaciovera/Desktop/Codigos/SER-Raspberry/Ser-Raspberry/'):
    if(dia[5:7]==f[5:7]):
        ExcelDia.append(f)
ExcelDia= sorted(ExcelDia)
print(ExcelDia)

# Guardamos en el fichero "datos2.txt" (creándolo) dos columnas
# que contienen los arrays x_datos e y_datos
np.savetxt("datos2.txt", (unos))

# Leemos el fichero que acabamos de crear y
# almacenamos los arrays en x e y
#x, y = np.loadtxt("datos2.txt")