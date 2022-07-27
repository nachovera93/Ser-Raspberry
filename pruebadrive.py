import gspread
import datetime
import numpy as np
import json
gc = gspread.service_account(filename='rep_medidor.json')
sh = gc.open('Luis_Wherhahm')
datetim=datetime.datetime.now()

worksheet = sh.worksheet("Hoja 1")
datetim=datetime.datetime.now()-datetime.timedelta(minutes=3)
datetim=json.dumps(datetim.time(), default=str)
#=[f'{datetim.hour}:{datetim.minute}{datetim.minute}']

#worksheet.update('B13',datetim[1:6])
worksheet.batch_clear(["A11:A12"])


