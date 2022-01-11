import time
from datetime import datetime


fecha=str(datetime.now())
time.sleep(40)

print("Hello Bash")

f = open('mi_fichero.txt', 'w')
try:
    f.write(fecha)
finally:
    f.close()