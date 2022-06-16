#from ast import For
import numpy as np
from openpyxl import Workbook
from openpyxl.chart import (
    AreaChart,
    Reference,
    Series,
)

from datetime import date
from datetime import time
import openpyxl
dest_filename = f'{date.today()}.xlsx'
workbook=openpyxl.load_workbook(filename = dest_filename)
sheet11 = workbook[f"Maximos por hora"]

print("Ultima columna", (sheet11.max_column))     

codigos0 = [celda[0].value for celda in sheet11['A3':f'A{sheet11.max_column}']]
horas=[]
for hour in codigos0:
    hora=hour.hour
    my_time = time(hora, 0, 0)
    horas.append(my_time)
    
#Separar fases en excel
codigos1 = [celda[0].value for celda in sheet11['B3':f'B{sheet11.max_column}']]
codigos2 = [celda[0].value for celda in sheet11['C3':f'C{sheet11.max_column}']]
codigos3 = [celda[0].value for celda in sheet11['D3':f'D{sheet11.max_column}']]
codigos4 = [celda[0].value for celda in sheet11['E3':f'E{sheet11.max_column}']]
codigos5 = [celda[0].value for celda in sheet11['F3':f'F{sheet11.max_column}']]
codigos6 = [celda[0].value for celda in sheet11['G3':f'G{sheet11.max_column}']]
codigos7 = [celda[0].value for celda in sheet11['H3':f'H{sheet11.max_column}']]
codigos8 = [celda[0].value for celda in sheet11['I3':f'I{sheet11.max_column}']]
codigos9 = [celda[0].value for celda in sheet11['J3':f'J{sheet11.max_column}']]


import pandas as pd
     
df1 = pd.DataFrame({'Hora':horas,'Energia1':codigos1,
                                 'Energia2':codigos2,
                                 'Energia3':codigos3,
                                 'Energia4':codigos4,
                                 'Energia5':codigos5,
                                 'Energia6':codigos6,
                                 'Energia7':codigos7,
                                 'Energia8':codigos8,
                                 'Energia9':codigos9})
                    
#new_df=df.assign(Profit=6)
print(df1)
#arr = np.array(df)


wb = Workbook()
ws = wb.active
headings3=list(['Hora','Energia-Fase-1-REDCompañia', 'Energia-Fase-1-CentralFotovoltaica','Energia-Fase-1-ConsumoCliente','Energia-Fase-2-REDCompañia','Energia-Fase-2-CentralFotovoltaica','Energia-Fase-2-ConsumoCliente','Energia-Fase-3-REDCompañia','Energia-Fase-3-CentralFotovoltaica','Energia-Fase-3-ConsumoCliente'])
ws.append(headings3)
df_list=df1.values.tolist()
arr = np.array(df1)
print(np.shape(arr))

for row in df_list:
    ws.append(row)

chart = AreaChart()
chart.title = "Energias Fase 1"
chart.style = 13
chart.x_axis.title = 'Hora'
chart.y_axis.title = 'KWh'

cats = Reference(ws, min_col=1, min_row=2, max_row=f'{sheet11.max_column+1}')
data = Reference(ws, min_col=2, min_row=1, max_col=f'{sheet11.max_column+1}', max_row=f'{sheet11.max_column+1}')
chart.add_data(data, titles_from_data=True)
chart.set_categories(cats)

ws.add_chart(chart, f"A{sheet11.max_column+1}")

wb.save("area.xlsx")



"""
ne creo
no
pienso que no
nunca
de ninguna manera
nope
no gracias
nada
jamás
equivocado
me declino
no me gusta eso
no lo creo
ehh no
no quiero aceptar
creo que me referia a que no
no estoy segurono lo conozco
no soy
no señor
nop
no señorita
nop, no soy
no equivocado
no quiero
ehhh no
no lo conozco
ah no
no lo cacho
nó
noooo
nooo
noooooo
te dije que no
te dije que nooo

el mismo
cierto
claro
correcto
eso suena bien
yes
si porfavor
porsupuesto
yup
yeah
yes please
seguro
ye
eso sería genial
positivo
afirmativo
Yup
Yea
Yeah
Yeah seguro
Yep
Yep suena bien
Sep!
Sipp
Sip
Si
si porfa
sii
eso
sip
ah si
si
sii porque?
si pero no está
si dime
si con el
ah bueno
si, con ella
si pero no esta
si con el mismo
si con quien
que si
sí
siiii
siii
sii
la verdad es que si
con el
con ella
ok
ti
tiii
mande
mandé
digame
diga
soy yo
si soy yo
y
i
yy 
cuenteme

hola
ola
alo
que tal?
Hola
Holaaaaaaaaaa
holaaaaaaa
alooooooo
olaaa
yo
buenos dias
hi
alo ahí
hey
buenas tardes
hello
alo?
hola estás ahi?
alo estas ahi?
Hey
hi!
hola tu
Holas
Bienvenido
Hola gertrudis
holas
Alo
"""