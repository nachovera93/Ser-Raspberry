import sys
from timeit import default_timer 
import os
import time

def BorrarArchivos():
      comienzo=default_timer()
      #current_time = time.time()
      print() 
      count=0
      for f in os.listdir("images/señal/voltage/"):
                 #print(f)
                 count=count+1
                 
      print(count)
      if(count>10):
            for i in os.listdir("images/señal/voltage"):
                      print(i)
                      os.remove(f'images/señal/voltage/{i}')
                         


BorrarArchivos()