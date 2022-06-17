from ast import For
import numpy as np
from openpyxl import Workbook
from openpyxl.chart import (
    AreaChart,
    Reference,
    Series,
)
import datetime
from datetime import date
from datetime import time
import openpyxl
dest_filename = '2022-06-16.xlsx'
workbook=openpyxl.load_workbook(filename = dest_filename)
sheet11 = workbook["MaxHora Fase 1"]
sheet12 = workbook["MaxHora Fase 2"]
sheet13 = workbook["MaxHora Fase 3"]

#print("Ultima columna", len(sheet11['A']))    
codigos0 = [celda[0].value for celda in sheet11['A2':f"A{len(sheet11['A']) }"]] #{sheet11.max_column}
print(codigos0)
#horas=[]
#for hour in codigos0:
#    print(hour)
#    hora=hour.hour
#    my_time = time(hora, 0, 0)
    #horas.append(f'{datetim.hour}:{datetim.minute}')
#print(len(horas))  
#Separar fases en excel
codigos1 = [celda[0].value for celda in sheet11['B2':f"B{len(sheet11['A']) }"]]
codigos2 = [celda[0].value for celda in sheet11['C2':f"C{len(sheet12['A']) }"]]
codigos3 = [celda[0].value for celda in sheet11['D2':f"D{len(sheet13['A']) }"]]
codigos4 = [celda[0].value for celda in sheet12['B2':f"B{len(sheet11['A']) }"]]
codigos5 = [celda[0].value for celda in sheet12['C2':f"C{len(sheet12['A']) }"]]
codigos6 = [celda[0].value for celda in sheet12['D2':f"D{len(sheet13['A']) }"]]
codigos7 = [celda[0].value for celda in sheet13['B2':f"B{len(sheet11['A']) }"]]
codigos8 = [celda[0].value for celda in sheet13['C2':f"C{len(sheet12['A']) }"]]
codigos9 = [celda[0].value for celda in sheet13['D2':f"D{len(sheet13['A']) }"]]
"""
import pandas as pd 
df1 = pd.DataFrame({'Hora':horas,'Energia1':codigos1,
                                 'Energia2':codigos2,
                                 'Energia3':codigos3
                                 })
df2 = pd.DataFrame({'Hora':horas,'Energia4':codigos4,
                                 'Energia5':codigos5,
                                 'Energia6':codigos6
                                 })
df3 = pd.DataFrame({'Hora':horas,'Energia7':codigos7,
                                 'Energia8':codigos8,
                                 'Energia9':codigos9
                                 })
"""
#new_df=df.assign(Profit=6)
#print(df1)
#print(df2)
#print(df3)
#arr = np.array(df)


#wb = Workbook()
#ws = wb.active
#headings1=list(['Hora','Energia-Fase-1-REDCompañia', 'Energia-Fase-1-CentralFotovoltaica','Energia-Fase-1-ConsumoCliente'])
#headings2=list(['Hora','Energia-Fase-1-REDCompañia', 'Energia-Fase-1-CentralFotovoltaica','Energia-Fase-1-ConsumoCliente'])
#headings3=list(['Hora','Energia-Fase-1-REDCompañia', 'Energia-Fase-1-CentralFotovoltaica','Energia-Fase-1-ConsumoCliente'])#,'Energia-Fase-2-REDCompañia','Energia-Fase-2-CentralFotovoltaica','Energia-Fase-2-ConsumoCliente','Energia-Fase-3-REDCompañia','Energia-Fase-3-CentralFotovoltaica','Energia-Fase-3-ConsumoCliente'])
#sheet11.append(headings1)
#sheet12.append(headings2)
#sheet13.append(headings3)
"""
df_list1=df1.values.tolist()
df_list2=df2.values.tolist()
df_list3=df3.values.tolist()
arr = np.array(df1)
print(np.shape(arr))

for row in df_list1:
    sheet11.append(row)
for row in df_list2:
    sheet12.append(row)
for row in df_list3:
    sheet13.append(row)
"""
chart = AreaChart()
chart.title = "Energias Fase 1"
chart.style = 13
chart.x_axis.title = 'Hora'
chart.y_axis.title = 'KWh'

cats = Reference(sheet11, min_col=1, min_row=2, max_row=f'{sheet11.max_column+1}')
data = Reference(sheet11, min_col=2, min_row=1, max_col=f'{sheet11.max_column+1}', max_row=f'{sheet11.max_column+1}')
chart.add_data(data, titles_from_data=True)
chart.set_categories(cats)

sheet11.add_chart(chart, f"A{sheet11.max_column+1}")

workbook.save("2022-06-16.xlsx")



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
