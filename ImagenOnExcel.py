from ast import For
import numpy as np
from openpyxl import Workbook
from openpyxl.chart import (
    AreaChart,
    Reference,
    Series,
)
import openpyxl

workbook=openpyxl.load_workbook(filename = "2022-06-03.xlsx")
sheet19 = workbook[f"Var 9"]
LargeSheet11=len(sheet19["FP"])
codigos = [celda[0].value for celda in sheet19['M2':f'M{LargeSheet11}']]
numbers=[]
j=0
for number in codigos:
    numbers.append(j-1)
    j=j+1
    
numbers=numbers[1:len(numbers)]
import pandas as pd
#
#dates=['April-10', 'April-11', 'April-12', 'April-13']
#fruits=['Apple', 'Papaya', 'Banana', 'Mango']
#prices=[3, 1, 2, 4]


     
df = pd.DataFrame({'Numero':numbers,'Energia':codigos[1:len(codigos)]})

#new_df=df.assign(Profit=6)
print(df)
#arr = np.array(df)


wb = Workbook()
ws = wb.active

rows = [
    [2, 40],
    [3, 40],
    [4, 50],
    [5, 30],
    [6, 25],
    [7, 50],
]
df_list=df.values.tolist()
arr = np.array(df)
print(type(rows))
print(type(df_list))
print(np.shape(arr))

for row in df_list:
    ws.append(row)

chart = AreaChart()
chart.title = "Area Chart"
chart.style = 13
chart.x_axis.title = 'Test'
chart.y_axis.title = 'Percentage'

cats = Reference(ws, min_col=1, min_row=1, max_row=7)
data = Reference(ws, min_col=2, min_row=1, max_col=3, max_row=7)
chart.add_data(data, titles_from_data=True)
chart.set_categories(cats)

ws.add_chart(chart, "A10")

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