import requests
from datetime import date
from datetime import datetime
import json
import xlsxwriter
import random
import time
import paho.mqtt.client as mqtt
import os
import threading
import datetime
from scipy import interpolate
from scipy.fft import fft, fftfreq
from scipy.interpolate import lagrange
from scipy import signal
from scipy.signal import savgol_filter
import numpy as np
import subprocess
from time import sleep
import sys
import socket
import RPi.GPIO as GPIO
import time
import glob
import os
import serial
import math
import openpyxl
import smtplib, ssl
import getpass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import datetime
import matplotlib.pyplot as plt
import collections
import psutil
import gzip
"""
    0: connection succeeded
    1: connection failed - incorrect protocol version
    2: connection failed - invalid client identifier
    3: connection failed - the broker is not available
    4: connection failed - wrong username or password
    5: connection failed - unauthorized
    6-255: undefined
"""



def BorrarArchivos():

      current_time = time.time()
      for f in os.listdir():
            creation_time = os.path.getctime(f)
            if (((current_time - creation_time) // (24 * 3600)) >= 7):
                os.unlink(f)

#BorrarArchivos()


try:                           
     esp32 = serial.Serial('/dev/ttyUSB0', 230400, timeout=0.5)
     esp32.flushInput()
except:
     esp32 = serial.Serial('/dev/ttyUSB1', 230400, timeout=0.5)
     esp32.flushInput()
     
horasetup=datetime.datetime.now()
print("Hora de comienzo:", horasetup)

broker = '18.228.175.193'    #mqtt server
port = 1883
dId = '123454321'
passw = '25ebiBqWgR'
webhook_endpoint = 'http://18.228.175.193:3001/api/getdevicecredentials'


def get_mqtt_credentials():
    global usernamemqtt
    global passwordmqtt
    global mqttopic
    global str_client_id
    global topicmqtt
    global data
    print("Getting MQTT Credentials from WebHook")
    time.sleep(2)
    toSend = {"dId": dId, "password": passw}
    respuesta = requests.post(webhook_endpoint, data=toSend)

    if(respuesta.status_code < 0):
          print("Error Sending Post Request ", respuesta.status_code)
          respuesta.close()
          return False
    if(respuesta.status_code != 200):
          print("Error in response ", respuesta.status_code)
          respuesta.close()
          return False
    if(respuesta.status_code == 200):
          print("Mqtt Credentials Obtained Successfully :)   ")
          #print("json: " ,resp.content)
          #print('- ' * 20)
          my_bytes_value = respuesta.content      #Contenido entero del json
          my_new_string = my_bytes_value.decode("utf-8").replace("'", '"')
          data = json.loads(my_new_string)
          s = json.dumps(data, indent=4, sort_keys=True)
          print(s)
          usernamemqtt = data["username"]
          #print("username:",usernamemqtt)
          passwordmqtt = data["password"]
          topicmqtt = data["topic"]   #topico al que nos vamos a suscribir
          mqttopic = f"{topicmqtt}+/actdata"
          str_client_id = f'device_{dId}_{random.randint(0, 9999)}'
          #print(mqttopic)
          respuesta.close()
          print("Ends mqtt credentials")
    return True

   
def on_disconnect(client, userdata, rc):
    if (rc != 0 and rc != 5):
        print("Unexpected disconnection, will auto-reconnect")
    elif(rc==5):
        print("Getting new credentials!")
        get_mqtt_credentials()
        client.username_pw_set(usernamemqtt, passwordmqtt)
                      
def on_connected(client, userdata, flags, rc):
  
    if rc==0:
        client.connected_flag=True #set flag
        client.subscribe(mqttopic)
        print("connected OK")
        print("rc =",client.connected_flag)
    else:
        print("Bad connection Returned code=",rc)
        client.bad_connection_flag=False

   
"""
get_mqtt_credentials()     
client = mqtt.Client(str_client_id)   #Creación cliente
client.connect(broker, port)     #Conexión al broker
client.on_disconnect = on_disconnect
client.username_pw_set(usernamemqtt, passwordmqtt)
client.on_connect = on_connected
client.loop_start()

def reconnectmqtt():
    get_mqtt_credentials()     
    client = mqtt.Client(str_client_id)   #Creación cliente
    client.connect(broker, port)     #Conexión al broker
    client.on_disconnect = on_disconnect
    client.username_pw_set(usernamemqtt, passwordmqtt)
    client.on_connect = on_connected
    client.loop_start()
"""

def get_cpuload():
    cpuload = psutil.cpu_percent(interval=1, percpu=False)
    return str(cpuload)

def cpu_temp():
	thermal_zone = subprocess.Popen(
	    ['cat', '/sys/class/thermal/thermal_zone0/temp'], stdout=subprocess.PIPE)
	out, err = thermal_zone.communicate()
	cpu_temp = int(out.decode())/1000
	return cpu_temp

CPU_temp = 0.0
EstateVentilador="OFF"

def Ventilador():
    global EstateVentilador
    CPU_temp = round(cpu_temp(),0)
    if CPU_temp > 53:
        GPIO.output(23, True)
        str_num = {"value":"ON","save":0}
        EstateVentilador = json.dumps(str_num)
    elif CPU_temp <= 38:
        GPIO.output(23, False)
        str_num = {"value":"OFF","save":0}
        EstateVentilador = json.dumps(str_num)

def getMaxValues(myList, quantity):
        return(sorted(list(set(myList)), reverse=True)[:quantity]) 
        #print(f'max : {max(myList)}')

def getMinValues(myList, quantity):
        return(sorted(list(set(myList)))[:quantity]) 
        #print(f'max : {max(myList)}')

Vrms=0.0
def VoltRms(MaxVoltage2):
    if(MaxVoltage2<13):
        Vrms=0
        #print(f'Vrms : {Vrms}')
        return Vrms
    Vrms=MaxVoltage2*0.71
    #Vrms=(-1.16 + 0.179*(MaxVoltage2) +  -0.00718*(MaxVoltage2**2) + 0.000155*(MaxVoltage2**3) + -0.00000203*(MaxVoltage2**4) + 0.0000000168*(MaxVoltage2**5) + -0.0000000000912*(MaxVoltage2**6) + 0.00000000000032*(MaxVoltage2**7) + -0.000000000000000703*(MaxVoltage2**8) + 0.000000000000000000875*(MaxVoltage2**9) + -0.000000000000000000000472*(MaxVoltage2**10))*MaxVoltage2
    #print(f'Vrms : {Vrms}')
    return Vrms

Irms=0.0
def CurrentRms(MaxCurrent2):
    #Irms=(-0.0248 + 0.00402*MaxCurrent2 - 0.000176*(MaxCurrent2**2) + 0.00000392*(MaxCurrent2**3) - 0.000000046*(MaxCurrent2**4) + 0.000000000284*(MaxCurrent2**5) - 0.00000000000069*(MaxCurrent2**6))*MaxCurrent2
    #print(f'Irms : {Irms}')
    if(MaxCurrent2>430):
         Irms = MaxCurrent2*0.0133
    else:
         Irms=(0.0046 + 0.000282*MaxCurrent2 - 0.00000328*(MaxCurrent2**2) + 0.0000000167*(MaxCurrent2**3) - 0.0000000000382*(MaxCurrent2**4) + 0.0000000000000322*(MaxCurrent2**5))*MaxCurrent2
    
    return Irms



Vrms0=0.0
def VoltajeRms(listVoltage):
    global Vrms0
    N = len(listVoltage)
    Squares = []

    for i in range(0,N,1):    #elevamos al cuadrado cada termino y lo amacenamos
         listsquare = listVoltage[i]*listVoltage[i]
         Squares.append(listsquare)
    
    SumSquares=0
    for i in range(0,N,1):    #Sumatoria de todos los terminos al cuadrado
         SumSquares = SumSquares + Squares[i]

    MeanSquares = (1/N)*SumSquares #Dividimos por N la sumatoria

    Vrms0=np.sqrt(MeanSquares)
   
    #print(f'Voltaje RMS : {Vrms0}')
    return Vrms0


Irms0=0.0
def CorrienteRms(listCurrent):
    global Irms0
    
    N = len(listCurrent)
    Squares = []

    for i in range(0,N,1):    #elevamos al cuadrado cada termino y lo amacenamos
         listsquare = listCurrent[i]*listCurrent[i]
         Squares.append(listsquare)
    
    SumSquares=0
    for i in range(0,N,1):    #Sumatoria de todos los terminos al cuadrado
         SumSquares = SumSquares + Squares[i]

    MeanSquares = (1/N)*SumSquares #Dividimos por N la sumatoria

    Irms0=np.sqrt(MeanSquares)
    
    #print(f'Corriente RMS : {Irms0}')
    return Irms0

#potrms=0.0
def PotenciaRms(listCurrent,listVoltage):
    global Squares
    
    N = len(listCurrent)
    Squares = []

    for i in range(0,N,1):    #elevamos al cuadrado cada termino y lo amacenamos
         listsquare = listCurrent[i]*listVoltage[i]
         Squares.append(listsquare)
    
    SumSquares=0
    for i in range(0,N,1):    #Sumatoria de todos los terminos al cuadrado
         SumSquares = SumSquares + Squares[i]

    MeanSquares = (1/N)*SumSquares #Dividimos por N la sumatoria

    #potrms=np.sqrt(MeanSquares)
    
    
    return MeanSquares

    

FDVoltage_1 = 0.0
DATVoltage_1= 0.0
FDVoltage_2 = 0.0
DATVoltage_2= 0.0
FDVoltage_3 = 0.0
DATVoltage_3= 0.0
FDVoltage_4 = 0.0
DATVoltage_4= 0.0
FDVoltage_5 = 0.0
DATVoltage_5= 0.0
FDVoltage_6 = 0.0
DATVoltage_6= 0.0
FDVoltage_7 = 0.0
DATVoltage_7= 0.0
FDVoltage_8 = 0.0
DATVoltage_8= 0.0
FDVoltage_9 = 0.0
DATVoltage_9= 0.0
sincvoltaje=0
PhaseVoltage=0.0
def VoltageFFT(list_fftVoltages, samplings,i):
    global PhaseVoltage
    global j
    global FDVoltage_1 
    global DATVoltage_1
    global FDVoltage_2
    global DATVoltage_2
    global FDVoltage_3
    global DATVoltage_3
    global FDVoltage_4 
    global DATVoltage_4
    global FDVoltage_5 
    global DATVoltage_5
    global FDVoltage_6 
    global DATVoltage_6
    global FDVoltage_7 
    global DATVoltage_7
    global FDVoltage_8 
    global DATVoltage_8
    global FDVoltage_9 
    global DATVoltage_9
    global sincvoltaje1
    p = i
    N = len(list_fftVoltages)
    T = 1 / samplings
    list_fftVoltages -= np.mean(list_fftVoltages)
    datosfft = list_fftVoltages * np.hamming(4096)
    yf = np.fft.rfft(datosfft)
    xf = fftfreq(N, T)[:N//2] 
    if (samplings > 5100):
           f = interpolate.interp1d(xf, yf[:N//2] )
           xnewv = np.arange(0, 2575, 1)  # 2550
           ynew = f(xnewv)
           ejeyabsolutv =  2.0/4096 * np.abs(ynew)
           FD = []
           complejo = []
           real=[]
           imag=[]
           z=0
           for i in range(45, 2575, 50):
                 Time1b = max(ynew[i:i+10])
                 arra = max(ejeyabsolutv[i:i+10])
                 complejo.append(Time1b)
                 real1 = Time1b.real
                 real.append(real1)
                 imag1 = Time1b.imag
                 imag.append(imag1)
                 z=z+1
                 FD.append(arra)
           FD2=[]       
           for i in range(0,len(FD)):
               if(FD[i]>(FD[0]/10)):
                   FD2.append(FD[i])
                   
           SumMagnitudEficaz = (np.sum([FD[0:len(FD)]]))
           Magnitud1 = FD[0]            
           PhaseVoltage = np.arctan(real[0]/(imag[0]))
           FDVoltage = Magnitud1/SumMagnitudEficaz
           DATVoltage= np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
           sincvoltaje1 = 1
           str_num_FD = {"value":FDVoltage,"save":1}
           str_num_DAT = {"value":DATVoltage,"save":1}
           print("SincVoltaje Voltage: ", sincvoltaje1)
           if (p == 1):
               print("FD: ",FDVoltage)
               FDVoltage_1 = FDVoltage
               DATVoltage_1= DATVoltage
               FDVoltage_1_JSON = json.dumps(str_num_FD)  
               DATVoltage_1_JSON = json.dumps(str_num_DAT)
           elif (p == 2):
               FDVoltage_2 = FDVoltage
               DATVoltage_2= DATVoltage
               FDVoltage_2_JSON = json.dumps(str_num_FD)  
               DATVoltage_2_JSON = json.dumps(str_num_DAT)             
           elif (p == 3):
               FDVoltage_3 = FDVoltage
               DATVoltage_3= DATVoltage
               FDVoltage_3_JSON = json.dumps(str_num_FD)  
               DATVoltage_3_JSON = json.dumps(str_num_DAT)             
           elif (p == 4):
               FDVoltage_4 = FDVoltage
               DATVoltage_4= DATVoltage
               FDVoltage_4_JSON = json.dumps(str_num_FD)  
               DATVoltage_4_JSON = json.dumps(str_num_DAT)             
           elif (p == 5):
               FDVoltage_5 = FDVoltage
               DATVoltage_5= DATVoltage
               FDVoltage_5_JSON = json.dumps(str_num_FD)  
               DATVoltage_5_JSON = json.dumps(str_num_DAT)             
           elif (p == 6):
               FDVoltage_6 = FDVoltage
               DATVoltage_6= DATVoltage
               FDVoltage_6_JSON = json.dumps(str_num_FD)  
               DATVoltage_6_JSON = json.dumps(str_num_DAT)             
           elif (p == 7):
               FDVoltage_7 = FDVoltage
               DATVoltage_7= DATVoltage
               FDVoltage_7_JSON = json.dumps(str_num_FD)  
               DATVoltage_7_JSON = json.dumps(str_num_DAT)             
           elif (p == 8):
               FDVoltage_8 = FDVoltage
               DATVoltage_8= DATVoltage
               FDVoltage_8_JSON = json.dumps(str_num_FD)  
               DATVoltage_8_JSON = json.dumps(str_num_DAT)             
           elif (p == 9):
               FDVoltage_9 = FDVoltage
               DATVoltage_9= DATVoltage
               FDVoltage_9_JSON = json.dumps(str_num_FD)  
               DATVoltage_9_JSON = json.dumps(str_num_DAT)             
           


CosPhi_1= 0.0
FP_1= 0.0
DATCurrent_1= 0.0
FDCurrent_1= 0.0
CoFP_2= 0.0
DATCurrent_2= 0.0
FDCurrent_2= 0.0
CosPhi_3= 0.0
FP_3= 0.0
DATCurrent_3= 0.0
FDCurrent_3= 0.0
CosPhi_4= 0.0
FP_4= 0.0
DATCurrent_4= 0.0
FDCurrent_4= 0.0
CosPhi_5= 0.0
FP_5= 0.0
DATCurrent_5= 0.0
FDCurrent_5= 0.0
CosPhi_6= 0.0
FP_6= 0.0
DATCurrent_6= 0.0
FDCurrent_6= 0.0
CosPhi_7= 0.0
FP_7= 0.0
DATCurrent_7= 0.0
FDCurrent_7= 0.0
CosPhi_8= 0.0
FP_8= 0.0
DATCurrent_8= 0.0
FDCurrent_8= 0.0
CosPhi_9= 0.0
FP_9= 0.0
DATCurrent_9= 0.0
FDCurrent_9= 0.0
CosPhi=0.0
PhaseCurrent=0.0

def CurrentFFT(list_fftVoltages, samplings, i,Irms):
    global CosPhi
    global CosPhi_1
    global FP_1
    global DATCurrent_1
    global FDCurrent_1
    global CosPhi_2
    global FP_2
    global DATCurrent_2
    global FDCurrent_2
    global CosPhi_3
    global FP_3
    global DATCurrent_3
    global FDCurrent_3
    global CosPhi_4
    global FP_4
    global DATCurrent_4
    global FDCurrent_4
    global CosPhi_5
    global FP_5
    global DATCurrent_5
    global FDCurrent_5
    global CosPhi_6
    global FP_6
    global DATCurrent_6
    global FDCurrent_6
    global CosPhi_7
    global FP_7
    global DATCurrent_7
    global FDCurrent_7
    global CosPhi_8
    global FP_8
    global DATCurrent_8
    global FDCurrent_8
    global CosPhi_9
    global FP_9
    global DATCurrent_9
    global FDCurrent_9
    global PhaseCurrent
    global sincvoltaje1
    q = i
    N = len(list_fftVoltages)
    T = 1 / samplings
    list_fftVoltages -= np.mean(list_fftVoltages)
    datosfft = list_fftVoltages * np.hamming(4096)
    yf = np.fft.rfft(datosfft)
    
    xf = fftfreq(N, T)[:N//2]
    if (samplings > 5100):
         f = interpolate.interp1d(xf,yf[:N//2])
         xnew = np.arange(0, 2575, 1)
         ynew = f(xnew)
         ejeyabsolut =  2.0/N * np.abs(ynew)
         p = int(i)
         z=0
         FD= []
         complejo = []
         real=[]
         imag=[]
         for i in range(45, 2575, 50):
               Time1b = max(ynew[i:i+10])
               arra = max(ejeyabsolut[i:i+10])
               complejo.append(Time1b)
               real1 = Time1b.real
               real.append(real1)
               imag1 = Time1b.imag
               imag.append(imag1)
               FD.append(arra)
         repite = list(ejeyabsolut)
         #SumMagnitudEficaz = (np.sum([FD[0:len(FD)]]))*0.01
         SumMagnitudEficaz2 = (np.sum([FD[0:len(FD)]]))
         Magnitud1 = FD[0]#*0.01
         ArmonicosRestantes=SumMagnitudEficaz2-Magnitud1
         proporcion = Irms/(np.sqrt(Magnitud1**2+ArmonicosRestantes**2))
         Irmsarmonico1prop=Magnitud1*proporcion
         Irmstotalproporcionado=np.sqrt((Irmsarmonico1prop**2)+(ArmonicosRestantes*proporcion)**2)
         FDCurrent = Irmsarmonico1prop/Irms
         str_num_FD_Current = {"value":FDCurrent,"save":0}
         JsonFDCurrent = json.dumps(str_num_FD_Current)
         DATCurrent = np.sqrt((SumMagnitudEficaz2**2-Magnitud1**2)/(Magnitud1**2))
         str_num_DAT_Current = {"value":DATCurrent,"save":0}
         JsonDATCurrent = json.dumps(str_num_DAT_Current)
         PhaseCurrent = np.arctan(real[0]/(imag[0]))
         if (sincvoltaje1 == 1):
             if(PhaseVoltage-(PhaseCurrent)>=0):
                 desfaseCGE = "Corriente Adelantada a Voltaje"
             else:
                 desfaseCGE = "Voltaje Adelantado a Corriente"
             FP=np.cos(PhaseVoltage-PhaseCurrent)*FDCurrent
             CosPhi=np.cos(PhaseVoltage-PhaseCurrent)
             str_num_FP = {"value":FP,"save":0}
             JsonFP = json.dumps(str_num_FP)
             sincvoltaje1=0  
             if (p == 1):
                 CosPhi_1=CosPhi
                 FP_1=FP
                 DATCurrent_1=DATCurrent
                 FDCurrent_1=FDCurrent
                 FDCurrentJson1 = json.dumps(str_num_FD_Current)  
                 DATCurrentJson1 = json.dumps(str_num_DAT_Current)
                 FPCurrentJson1 = json.dumps(str_num_FP) 
             elif (p == 2):
                 CosPhi_2=CosPhi
                 FP_2=FP
                 DATCurrent_2=DATCurrent
                 FDCurrent2=FDCurrent
                 FDCurrentJson2 = json.dumps(str_num_FD_Current)  
                 DATCurrentJson2 = json.dumps(str_num_DAT_Current)  
                 FPCurrentJson2 = json.dumps(str_num_FP)            
             elif (p == 3):
                 CosPhi_3=CosPhi
                 FP_3=FP
                 DATCurrent_3=DATCurrent
                 FDCurrent_3=FDCurrent
                 FDCurrentJson3 = json.dumps(str_num_FD_Current)  
                 DATCurrentJson3 = json.dumps(str_num_DAT_Current)
                 FPCurrentJson3 = json.dumps(str_num_FP)              
             elif (p == 4):
                 CosPhi_4=CosPhi
                 FP_4=FP
                 DATCurrent_4=DATCurrent
                 FDCurrent_4=FDCurrent
                 FDCurrentJson4 = json.dumps(str_num_FD)  
                 DATCurrentJson4 = json.dumps(str_num_DAT)
                 FPCurrentJson4 = json.dumps(str_num_FP)              
             elif (p == 5):
                 CosPhi_5=CosPhi
                 FP_5=FP
                 DATCurrent_5=DATCurrent
                 FDCurrent_5=FDCurrent
                 FDCurrentJson5 = json.dumps(str_num_FD)  
                 DATCurrentJson5 = json.dumps(str_num_DAT)
                 FPCurrentJson5 = json.dumps(str_num_FP)              
             elif (p == 6):
                 CosPhi_6=CosPhi
                 FP_6=FP
                 DATCurrent_6=DATCurrent
                 FDCurrent_6=FDCurrent
                 FDCurrentJson6 = json.dumps(str_num_FD)  
                 DATCurrentJson6 = json.dumps(str_num_DAT)
                 FPCurrentJson6 = json.dumps(str_num_FP)             
             elif (p == 7):
                 CosPhi_7=CosPhi
                 FP_7=FP
                 DATCurrent_7=DATCurrent
                 FDCurrent_7=FDCurrent
                 FDCurrentJson7 = json.dumps(str_num_FD)  
                 DATCurrentJson7 = json.dumps(str_num_DAT)
                 FPCurrentJson7 = json.dumps(str_num_FP)              
             elif (p == 8):
                 CosPhi_8=CosPhi
                 FP_8=FP
                 DATCurrent_8=DATCurrent
                 FDCurrent_8=FDCurrent
                 FDCurrentJson8 = json.dumps(str_num_FD)  
                 DATCurrent8son8 = json.dumps(str_num_DAT)
                 FPCurrentJson8 = json.dumps(str_num_FP)              
             elif (p == 9):
                 CosPhi_9=CosPhi
                 FP_9=FP
                 DATCurrent_9=DATCurrent
                 FDCurrent_9=FDCurren9
                 FDCurrentJson9 = json.dumps(str_num_FD)  
                 DATCurrentJson9 = json.dumps(str_num_DAT)
                 FPCurrentJson9 = json.dumps(str_num_FP)        


Time1a = datetime.datetime.now()
Time2a = datetime.datetime.now()
Time3a = datetime.datetime.now()
Time4a = datetime.datetime.now()
Time5a = datetime.datetime.now()
Time6a = datetime.datetime.now()
Time7a = datetime.datetime.now()
Time8a = datetime.datetime.now()
Time9a = datetime.datetime.now()
Energy_1 = 0.0
Energy_2 = 0.0
Energy_3 = 0.0
Energy_4 = 0.0
Energy_5 = 0.0
Energy_6 = 0.0
Energy_7 = 0.0
Energy_8 = 0.0
Energy_9 = 0.0
OneHourEnergy_1 = 0.0
OneHourEnergy_2 = 0.0
OneHourEnergy_3 = 0.0
OneHourEnergy_4 = 0.0
OneHourEnergy_5 = 0.0
OneHourEnergy_6 = 0.0
OneHourEnergy_7 = 0.0
OneHourEnergy_8 = 0.0
OneHourEnergy_9 = 0.0
AparentPower = 0.0
ActivePower = 0.0
ReactivePower = 0.0


def Potencias(i,Irms,Vrms,potrmsCGE):
    global a
    global Energy
    global Energy_1
    global Energy_2
    global Energy_3
    global Energy_4
    global Energy_5
    global Energy_6
    global Energy_7
    global Energy_8
    global Energy_9
    global OneHourEnergy_1
    global OneHourEnergy_2
    global OneHourEnergy_3
    global OneHourEnergy_4
    global OneHourEnergy_5
    global OneHourEnergy_6
    global OneHourEnergy_7
    global OneHourEnergy_8
    global OneHourEnergy_9
    global ActivePower
    global AparentPower
    global ReactivePower
    global Time1a
    global Time2a
    global Time3a
    global Time4a
    global Time5a
    global Time6a
    global Time7a
    global Time8a
    global Time9a
    TimeEnergy = datetime.datetime.now()
    if(TimeEnergy.minute==0):
            OneHourEnergy_1=0
            OneHourEnergy_2=0
            OneHourEnergy_3=0
            OneHourEnergy_4=0
            OneHourEnergy_5=0
            OneHourEnergy_6=0
            OneHourEnergy_7=0
            OneHourEnergy_8=0
            OneHourEnergy_9=0
    if(TimeEnergy.hour==0 and TimeEnergy.minute==0):
            Energy_1=0
            Energy_2=0
            Energy_3=0
            Energy_4=0
            Energy_5=0
            Energy_6=0
            Energy_7=0
            Energy_8=0
            Energy_9=0
    AparentPower = Vrms*Irms
    if (potrmsCGE>=0):
          ActivePower = Vrms*Irms*CosPhi
          ActivePower = np.abs(ActivePower)
    else:
          ActivePower = Vrms*Irms*CosPhi
          ActivePower = np.abs(ActivePower)
          ActivePower = ActivePower*(-1)
    ReactivePower = Vrms*Irms*np.sin(PhaseVoltage-PhaseCurrent)
    if (i == 1):
        Time1b = datetime.datetime.now()
        delta=(((Time1b - Time1a).microseconds)/1000+((Time1b - Time1a).seconds)*1000)/10000000000
        Energy_1 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_1 += np.abs(ActivePower*delta*2.9)
        Time1a = datetime.datetime.now()
        AparentPower_1 = AparentPower
        ActivePower_1 = ActivePower
        ReactivePower_1 = ReactivePower
        print(" i2 : ", i)
        SaveDataCsv(Vrms,Irms,ActivePower_1,ReactivePower_1,AparentPower_1,FP_1,CosPhi_1,FDVoltage_1,FDCurrent_1,DATVoltage_1,DATCurrent_1,Energy_1,OneHourEnergy_1,i)
        Maximo15min_1(Vrms,Irms,ActivePower_1,ReactivePower_1,AparentPower_1,FP_1,FDVoltage_1,FDCurrent_1,DATVoltage_1,DATCurrent_1,Energy_1)
    elif (i == 2):
        Time2b = datetime.datetime.now()
        delta=(((Time2b - Time2a).microseconds)/1000+((Time2b - Time2a).seconds)*1000)/10000000000
        Energy_2 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_2 += np.abs(ActivePower*delta*2.9)
        Time2a = datetime.datetime.now()
        AparentPower_2 = AparentPower
        ActivePower_2 = ActivePower
        ReactivePower_2 = ReactivePower 
        print(" i2 : ", i)
        SaveDataCsv(Vrms,Irms,ActivePower_2,ReactivePower_2,AparentPower_2,FP_2,CosPhi_2,FDVoltage_2,FDCurrent_2,DATVoltage_2,DATCurrent_2,Energy_2,OneHourEnergy_2,i)
        Maximo15min_2(Vrms,Irms,ActivePower_2,ReactivePower_2,AparentPower_2,FP_2,FDVoltage_2,FDCurrent_2,DATVoltage_2,DATCurrent_2,Energy_2)       
    elif (i == 3):
        Time3b = datetime.datetime.now()
        delta=(((Time3b - Time3a).microseconds)/1000+((Time3b - Time3a).seconds)*1000)/10000000000
        Energy_3 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_3 += np.abs(ActivePower*delta*2.9)
        Time3a = datetime.datetime.now()
        AparentPower_3 = AparentPower
        ActivePower_3 = ActivePower
        ReactivePower_3 = ReactivePower
        print(" i2 : ", i)
        SaveDataCsv(Vrms,Irms,ActivePower_3,ReactivePower_3,AparentPower_3,FP_3,CosPhi_3,FDVoltage_3,FDCurrent_3,DATVoltage_3,DATCurrent_3,Energy_3,OneHourEnergy_3,i)
        Maximo15min_3(Vrms,Irms,ActivePower_3,ReactivePower_3,AparentPower_3,FP_3,FDVoltage_3,FDCurrent_3,DATVoltage_3,DATCurrent_3,Energy_3)             
    elif (i == 4):
        Time4b = datetime.datetime.now()
        delta=(((Time4b - Time4a).microseconds)/1000+((Time4b - Time4a).seconds)*1000)/10000000000
        Energy_4 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_4 += np.abs(ActivePower*delta*2.9)
        Time4a = datetime.datetime.now()
        AparentPower_4 = AparentPower
        ActivePower_4 = ActivePower
        ReactivePower_4 = ReactivePower
        SaveDataCsv(Vrms,Irms,ActivePower_4,ReactivePower_4,AparentPower_4,FP_4,CosPhi_4,FDVoltage_4,FDCurrent_4,DATVoltage_4,DATCurrent_4,Energy_4,OneHourEnergy_4,i)
        Maximo15min_4(Vrms,Irms,ActivePower_4,ReactivePower_4,AparentPower_4,FP_4,FDVoltage_4,FDCurrent_4,DATVoltage_4,DATCurrent_4,Energy_4)              
    elif (i == 5):
        Time5b = datetime.datetime.now()
        delta=(((Time5b - Time5a).microseconds)/1000+((Time5b - Time5a).seconds)*1000)/10000000000
        Energy_5 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_5 += np.abs(ActivePower*delta*2.9)
        Time5a = datetime.datetime.now()
        AparentPower_5 = AparentPower
        ActivePower_5 = ActivePower
        ReactivePower_5 = ReactivePower
        SaveDataCsv(Vrms,Irms,ActivePower_5,ReactivePower_5,AparentPower_5,FP_5,CosPhi_5,FDVoltage_5,FDCurrent_5,DATVoltage_5,DATCurrent_5,Energy_5,OneHourEnergy_5,i)
        Maximo15min_5(Vrms,Irms,ActivePower_5,ReactivePower_5,AparentPower_5,FP_5,FDVoltage_5,FDCurrent_5,DATVoltage_5,DATCurrent_5,Energy_5)              
    elif (i == 6):
        Time6b = datetime.datetime.now()
        delta=(((Time6b - Time6a).microseconds)/1000+((Time6b - Time6a).seconds)*1000)/10000000000
        Energy_6 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_6 += np.abs(ActivePower*delta*2.9)
        Time6a = datetime.datetime.now()
        AparentPower_6 = AparentPower
        ActivePower_6 = ActivePower
        ReactivePower_6 = ReactivePower
        SaveDataCsv(Vrms,Irms,ActivePower_6,ReactivePower_6,AparentPower_6,FP_6,CosPhi_6,FDVoltage_6,FDCurrent_6,DATVoltage_6,DATCurrent_6,Energy_6,OneHourEnergy_6,i)
        Maximo15min_6(Vrms,Irms,ActivePower_6,ReactivePower_6,AparentPower_6,FP_6,FDVoltage_6,FDCurrent_6,DATVoltage_6,DATCurrent_6,Energy_6)           
    elif (i == 7):
        Time7b = datetime.datetime.now()
        delta=(((Time7b - Time7a).microseconds)/1000+((Time7b - Time7a).seconds)*1000)/10000000000
        Energy_7 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_7 += np.abs(ActivePower*delta*2.9)
        Time7a = datetime.datetime.now()
        AparentPower_7 = AparentPower
        ActivePower_7 = ActivePower
        ReactivePower_7 = ReactivePower
        SaveDataCsv(Vrms,Irms,ActivePower_7,ReactivePower_7,AparentPower_7,FP_7,CosPhi_7,FDVoltage_7,FDCurrent_7,DATVoltage_7,DATCurrent_7,Energy_7,OneHourEnergy_7,i)
        Maximo15min_7(Vrms,Irms,ActivePower_7,ReactivePower_7,AparentPower_7,FP_7,FDVoltage_7,FDCurrent_7,DATVoltage_7,DATCurrent_7,Energy_7)             
    elif (i == 8):
        Time8b = datetime.datetime.now()
        delta=(((Time8b - Time8a).microseconds)/1000+((Time8b - Time8a).seconds)*1000)/10000000000
        Energy_8 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_8 += np.abs(ActivePower*delta*2.9)
        Time8a = datetime.datetime.now()
        AparentPower_8 = AparentPowe7
        ActivePower_8 = ActivePower
        ReactivePower_8 = ReactivePower
        SaveDataCsv(Vrms,Irms,ActivePower_8,ReactivePower_8,AparentPower_8,FP_8,CosPhi_8,FDVoltage_8,FDCurrent_8,DATVoltage_8,DATCurrent_8,Energy_8,OneHourEnergy_8,i)
        Maximo15min_8(Vrms,Irms,ActivePower_8,ReactivePower_8,AparentPower_8,FP_8,FDVoltage_8,FDCurrent_8,DATVoltage_8,DATCurrent_8,Energy_8)            
    elif (i == 9):
        Time9b = datetime.datetime.now()
        delta=(((Time9b - Time9a).microseconds)/1000+((Time9b - Time9a).seconds)*1000)/10000000000
        Energy_9 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_9 += np.abs(ActivePower*delta*2.9)
        Time9a = datetime.datetime.now()
        AparentPower_9 = AparentPower
        ActivePower_9 = ActivePower
        ReactivePower_9 = ReactivePower
        SaveDataCsv(Vrms,Irms,ActivePower_9,ReactivePower_9,AparentPower_9,FP_9,CosPhi_9,FDVoltage_9,FDCurrent_9,DATVoltage_9,DATCurrent_9,Energy_9,OneHourEnergy_9,i)
        Maximo15min_9(Vrms,Irms,ActivePower_9,ReactivePower_9,AparentPower_9,FP_9,FDVoltage_9,FDCurrent_9,DATVoltage_9,DATCurrent_9,Energy_9) 
        """
          str_num = {"value":ActivePower,"save":1}
          str_num2 = {"value":ReactivePower,"save":0}
          str_num3 = {"value":AparentPower1,"save":1}
          str_num4 = {"value":Energy1 ,"save":0}
          ActivePower = json.dumps(str_num)
          AparentPower = json.dumps(str_num3)
          ReactivePower = json.dumps(str_num2)
          Energy = json.dumps(str_num4)
        
        """
    
    
font = {'family': 'serif',
        'color':  'darkred',
        'weight': 'normal',
        'size': 8,
        }

def graphVoltage(list_fftVoltage,list_FinalCurrent,samplings,i):
        global ax
        global imagenVoltaje
        i = str(i)
        global render
        fig=plt.figure(figsize=(8,6))
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)


        ax = fig.add_subplot(9,1,1)
        ax.plot(tiempoms,list_FinalCurrent,color="green", label="Corriente")
        if(i=="1"):
             plt.title(f'Corriente | I: {round(Irms,2)}  |  P-Activa: {round(ActivePower,2)} | P-Aparente: {round(AparentPower1,2)}  |  P-Reactiva:{round(ReactivePower1,2)}  ',fontdict=font)
        if(i=="2"):
             plt.title(f'Corriente | I: {round(Irms2,2)}  |  P-Activa: {round(Activa2Fase13,2)} | P-Aparente: {round(Aparente2Fase13,2)}  |  P-Reactiva:{round(Reactiva2Fase13,2)}  ',fontdict=font)
        if(i=="3"):
             plt.title(f'Corriente | I: {round(Irms3,2)}  |  P-Activa: {round(ActivaPanelesFase12,2)} | P-Aparente: {round(AparentePanelesFase12,2)}  |  P-Reactiva:{round(ReactivaPanelesFase12,2)}  ',fontdict=font)
             
        ax.legend(loc='upper left')
        ax.set_xlabel('Tiempo (mS)',fontdict=font)
        ax = fig.add_subplot(9,1,3)
        ax.plot(tiempoms,list_fftVoltage,color="blue", label="Voltaje")
        if(i=="1"):
             plt.title(f'Voltaje | V: {round(Vrms,2)} |  FP: {round(FP,2)}',fontdict=font)
        if(i=="2"):
             plt.title(f'Voltaje | V: {round(Vrms2,2)} |  FP: {round(FP21,2)}',fontdict=font)
        if(i=="3"):
             plt.title(f'Voltaje | V: {round(Vrms3,2)} |  FP: {round(FPPaneles1,2)}',fontdict=font)
        
        ax = fig.add_subplot(9,1,5)
        ax.plot(tiempoms,Squares,color="red", label="Pot-Activa")

        ax = fig.add_subplot(9,1,7)
        if(i=="1"):
            plt.title(f'FFT Corriente | DAT: {round(DATCurrent,2)}, FD: {round(FDCurrent,2)} |   cos phi: {round(CosPhi,2)} | phase voltaje CGE : {round(PhaseVoltage,2)} | phase Corriente CGE : {round(PhaseCurrent,2)}',fontdict=font)
        if(i=="2"):
            plt.title(f'FFT Corriente | DAT: {round(DATCorriente21,2)}, FD: {round(FDCorriente21,2)} |   cos phi: {round(cosphi2,2)} | phase voltaje 2 : {round(phasevoltaje2,2)} | phase Corriente 2 : {round(phasecorriente2,2)} ',fontdict=font)
        if(i=="3"):
            plt.title(f'FFT Corriente | DAT: {round(DATCorrientePaneles1,2)}, FD: {round(FDCorrientePaneles1,2)}  |   cos phi: {round(cosphiPaneles,2)} | phase voltaje Paneles: {round(phasevoltajePaneles,2)} | phase Corriente Paneles : {round(phasecorrientePaneles,2)}',fontdict=font)

        ax.plot(xnew,ejeyabsolut)
        rangex = np.zeros(28)
        n=0
        for h in range(50, 2600, 100):
           rangex[n]=h
           n = n+1
        ax.xaxis.set_ticks(rangex)  
        ax.grid(True)
        ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
        
        ax = fig.add_subplot(9,1,9)
        if(i=="1"):
            plt.title(f'{desfaseCGE}',fontdict=font)
        if(i=="2"):
            plt.title(f'{desfase2}',fontdict=font)
        if(i=="3"):
            plt.title(f'{desfasePaneles} ',fontdict=font)

        ax.plot(xnewv,ejeyabsolutv)
        rangex = np.zeros(28)
        n=0
        for h in range(50, 2600, 100):
           rangex[n]=h
           n = n+1
        ax.xaxis.set_ticks(rangex)  
        ax.grid(True)
        ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
        #ax.set_ylabel('|dB|',fontdict=font) 

        oldepoch = time.time()
        st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S') 
        #plt.xlabel("Tiempo(ms)",fontdict=font)
        #plt.title(f'FFT Voltaje |    DAT: {DATVoltage}     |     FD: {FDVoltage}    |   cos phi: {round(CosPhi,2)}',fontdict=font)
        ax.set_xlabel(f'Frecuencia (Hz)  |    fecha: {st} ',fontdict=font)
        #ax.set_ylabel('Pk-Pk',fontdict=font) 
        imagenVoltaje = f'images{i}/{st}.png'
        plt.savefig(imagenVoltaje)
        
    


b1=time.time()
c1=time.time()
d1=time.time()
e21=time.time()
f1=time.time()
g1=time.time()
h1=time.time()
j1=time.time()
k1=time.time()
z1=time.time()
l1=time.time()
m1=time.time()
n1=time.time()
o21=time.time()
p1=time.time()
q1=time.time()
r1=time.time()
s1=time.time()
t1=time.time()
u1=time.time()
v1=time.time()
v12=time.time()
v13=time.time()
v14=time.time()
v15=time.time()
v16=time.time()
v17=time.time()
w1=time.time()
x1=time.time()
y1=time.time()
#varsLastSend=[b,c,d,e]

def publish(client): 
        global b1, c1 ,d1, e21, f1 ,g1 ,h1 , j1, k1, l1, m1, n1, o21 ,p1, q1, r1, s1, t1, u1, v1 
        global v12, v13, v14, v15, v16, v17,v18,v19,v20,v21,v22,v23, w1, x1, y1, z1
        a1=time.time()
        for i in data["variables"]:

            #    if(data["variables"][i]["variableType"]=="output"):
            #        continue
            if(i["variableFullName"]=="Corriente-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - b1 > float(freq)):
                     b1=time.time()
                     str_variable = i["variable"]
                     topic1 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic1, Irms1)
                     status = result[0]            
                     if status == 0:
                         print(f"Send Irms: `{Irms1}` to topic `{topic1}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic1}")
        
                   
            if(i["variableFullName"]=="Voltaje-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - c1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     c1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, Vrms1)
                #     status = result[0]
                #     if status == 0:
                #         print(f"Send Vrms: `{Vrms1}` to topic `{topic2}` con freq: {freq}")
                #     else:
                #         print(f"Failed to send message to topic {topic2}")
            """
            if(i["variableFullName"]=="Potencia-Reactiva-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - d1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     d1=time.time()
                     str_variable3 = i["variable"]
                     topic3 = topicmqtt + str_variable3 + "/sdata"
                     result = client.publish(topic3, ReactivePower)
                     status = result[0]
                     if status == 0:
                         print(f"Send Pot-Reactiva-CGE: `{ReactivePower}` to topic `{topic3}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic3}")
            """
            if(i["variableFullName"]=="Pot-Activa-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - e21 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     e21=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, ActivePower)
               #      status = result[0]
               #      if status == 0:
               #          print(f"Send Pot-Activa CGE: `{ActivePower}` to topic `{topic4}` con freq: {freq}")
               #      else:
               #          print(f"Failed to send message to topic {topic4}")
            
            if(i["variableFullName"]=="Energia-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - f1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     f1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, Energy)
               #      status = result[0]
               #      if status == 0:
               #          print(f"Send energia CGE: `{Energy}` to topic `{topic5}` con freq: {freq}")
               #      else:
               #          print(f"Failed to send message to topic {topic5}")
            
            if(i["variableFullName"]=="FP-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - g1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     g1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FPCGE)
               #     status = result[0]
               #     if status == 0:
               #         print(f"Send FP-CGE: `{FPCGE}` to topic `{topic5}` con freq: {freq}")
               #     else:
               #         print(f"Failed to send message to topic {topic5}")
            """
            if(i["variableFullName"]=="FD-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - h1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     h1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, JsonFDCurrent)
                     status = result[0]
                     if status == 0:
                         print(f"Send FD-CGE: `{JsonFDCurrent}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="DAT-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - j1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     j1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, DATCorrienteCGE)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT-CGE: `{DATCorrienteCGE}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            """
            if(i["variableFullName"]=="Pot-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - k1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     k1=time.time()
                     str_variable = i["variable"]
                     topic5= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, AparentPower)
                     status = result[0]
                     if status == 0:
                         print(f"Send Pot-Aparente-CGE : `{AparentPower}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            
            #SEGUNDA TOMA
            if(i["variableFullName"]=="Corriente-2"):
                freq = i["variableSendFreq"]
                if(a1 - l1 > float(freq)):
                     l1=time.time()
                     str_variable = i["variable"]
                     topic = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, Irms22)
              #       status = result[0]
              #       
              #       if status == 0:
              #           print(f"Send Corriente-2: `{Irms22}` to topic `{topic}` con freq: {freq}")
              #       else:
              #           print(f"Failed to send message to topic {topic}")
        
                   
            if(i["variableFullName"]=="Voltaje-2"):
                freq = i["variableSendFreq"]
                if(a1 - m1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     m1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, Vrms22)
              #       status = result[0]
              #       if status == 0:
              #           print(f"Send Voltaje-2: `{Vrms22}` to topic `{topic2}` con freq: {freq}")
              #       else:
              #           print(f"Failed to send message to topic {topic2}")
            """
            if(i["variableFullName"]=="Potencia-Reactiva-2"):
                freq = i["variableSendFreq"]
                if(a1 - n1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     n1=time.time()
                     str_variable3 = i["variable"]
                     topic3 = topicmqtt + str_variable3 + "/sdata"
                     result = client.publish(topic3, Reactiva2Fase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Potencia-Reactiva-2: `{Reactiva2Fase1}` to topic `{topic3}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic3}")
            """
            if(i["variableFullName"]=="Pot-Activa-2"):
                freq = i["variableSendFreq"]
                if(a1 - o21 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     o21=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, Activa2Fase1)
               #      status = result[0]
               #      if status == 0:
               #          print(f"Send Pot-Activa-2: `{Activa2Fase1}` to topic `{topic4}` con freq: {freq}")
               #      else:
               #          print(f"Failed to send message to topic {topic4}")
            
            if(i["variableFullName"]=="Energia-2"):
                freq = i["variableSendFreq"]
                if(a1 - p1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     p1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energy2Fase1)
                 #    status = result[0]
                 #    if status == 0:
                 #        print(f"Send Energia-2: `{energy2Fase1}` to topic `{topic5}` con freq: {freq}")
                 #    else:
                 #        print(f"Failed to send message to topic {topic5}")
            
            if(i["variableFullName"]=="FP-2"):
                freq = i["variableSendFreq"]
                if(a1 - q1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     q1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FP2)
               #      status = result[0]
               #      if status == 0:
               #          print(f"Send FP-2: `{FP2}` to topic `{topic5}` con freq: {freq}")
               #      else:
               #          print(f"Failed to send message to topic {topic5}")
            """
            if(i["variableFullName"]=="FD-2"):
                freq = i["variableSendFreq"]
                if(a1 - r1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     r1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FDCorriente2)
                     status = result[0]
                     if status == 0:
                         print(f"Send FD-2: `{FDCorriente2}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="DAT-2"):
                freq = i["variableSendFreq"]
                if(a1 - s1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     s1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, DATCorriente2)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT-2: `{DATCorriente2}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            
            """
            if(i["variableFullName"]=="Pot-2"):
                freq = i["variableSendFreq"]
                if(a1 - t1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     t1=time.time()
                     str_variable = i["variable"]
                     topic5= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, Aparente2Fase1)
                     status = result[0]
              #       if status == 0:
               #          print(f"Send Pot-Aparente-2: `{Aparente2Fase1}` to topic `{topic5}` con freq: {freq}")
                #     else:
                 #        print(f"Failed to send message to topic {topic5}")
            
            #Tercera Toma
            if(i["variableFullName"]=="Corriente-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - u1 > float(freq)):
                     u1=time.time()
                     str_variable = i["variable"]
                     topic = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, Irms33)
               #      status = result[0]
               #      
               #      if status == 0:
               #          print(f"Send Corriente-Paneles: `{Irms33}` to topic `{topic}` con freq: {freq}")
               #      else:
               #          print(f"Failed to send message to topic {topic}")
        
                   
            if(i["variableFullName"]=="Voltaje-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, Vrms33)
                #     status = result[0]
                #     if status == 0:
                #         print(f"Send Voltaje-Paneles: `{Vrms33}` to topic `{topic2}` con freq: {freq}")
                #     else:
                #         print(f"Failed to send message to topic {topic2}")
            """
            if(i["variableFullName"]=="Potencia-Reactiva-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v12 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v12=time.time()
                     str_variable3 = i["variable"]
                     topic3 = topicmqtt + str_variable3 + "/sdata"
                     result = client.publish(topic3, ReactivaPanelesFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Potencia-Reactiva-Paneles: `{ReactivaPanelesFase1}` to topic `{topic3}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic3}")
            """
            if(i["variableFullName"]=="Pot-Activa-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v13 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v13=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, ActivaPanelesFase1)
                 #    status = result[0]
                 #    if status == 0:
                 #        print(f"Send Pot-Activa-Paneles: `{ActivaPanelesFase1}` to topic `{topic4}` con freq: {freq}")
                 #    else:
                 #        print(f"Failed to send message to topic {topic4}")
            
            if(i["variableFullName"]=="Energia-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v14 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v14=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energyPanelesFase1)
                  #   status = result[0]
                  #   if status == 0:
                  #       print(f"Send Energia-Paneles: `{energyPanelesFase1}` to topic `{topic5}` con freq: {freq}")
                  #   else:
                  #       print(f"Failed to send message to topic {topic5}")
            
            if(i["variableFullName"]=="FP-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v15 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v15=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FPPaneles)
                  #   status = result[0]
                  #   if status == 0:
                  #       print(f"Send FP-Paneles: `{FPPaneles}` to topic `{topic5}` con freq: {freq}")
                  #   else:
                  #       print(f"Failed to send message to topic {topic5}")
            """
            if(i["variableFullName"]=="FD-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v16 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v16=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FDCorrientePaneles)
                     status = result[0]
                     if status == 0:
                         print(f"Send FD-Paneles: `{FDCorrientePaneles}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="DAT-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v17 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v17=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, DATCorrientePaneles)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT-Paneles: `{DATCorrientePaneles}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="Pot-Aparente-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - w1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     w1=time.time()
                     str_variable = i["variable"]
                     topic5= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, AparentePanelesFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Pot-Aparente-Paneles: `{AparentePanelesFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            
            """
            if(i["variableFullName"]=="Voltaje-Baterias"):
                freq = i["variableSendFreq"]
                if(a1 - v16 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v16=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, VoltajeBateriaSend)  
                     status = result[0]
                     if status == 0:
                         print(f"Send Voltaje-Baterias: `{VoltajeBateriaSend}` to topic `{topic4}` ")
                     else:
                         print(f"Failed to send message to topic {topic4}")    
            if(i["variableFullName"]=="Corriente-Baterias"):
                freq = i["variableSendFreq"]
                if(a1 - v17 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v17=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, CorrienteBateriaSend)     
            if(i["variableFullName"]=="Potencia-Baterias"):
                freq = i["variableSendFreq"]
                if(a1 - v18 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v18=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, PotenciaBateriaSend)
            if(i["variableFullName"]=="Energia-Baterias"):
                freq = i["variableSendFreq"]
                if(a1 - v19 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v19=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energyBateriaSend)

            if(i["variableFullName"]=="Voltaje-PanelesDC"):
                freq = i["variableSendFreq"]
                if(a1 - v20 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v20=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, VoltajePanelesDCSend)

            if(i["variableFullName"]=="Corriente-PanelesDC"):
                freq = i["variableSendFreq"]
                if(a1 - v21 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v21=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, CorrientePanelesDCSend)    

            if(i["variableFullName"]=="Potencia-PanelesDC"):
                freq = i["variableSendFreq"]
                if(a1 - v22 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v22=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, PotenciaPanelesDCSend)
            if(i["variableFullName"]=="Energia-PanelesDC"):
                freq = i["variableSendFreq"]
                if(a1 - v23 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v23=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energyPanelesDCSend)

            if(i["variableFullName"]=="Temperatura-ESP32"):
                freq = i["variableSendFreq"]
                if(a1 - x1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     x1=time.time()
                     str_variable = i["variable"]
                     topic= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, tempESP32)
                     status = result[0]
                     #if status == 0:
                         #print(f"Send Temperatura-ESP32: `{tempESP32}` to topic `{topic}` con freq: {freq}")
                     #else:
                       #    print(f"Failed to send message to topic {topic}")

            if(i["variableFullName"]=="Ventilador-Raspberry"):
                freq = i["variableSendFreq"]
                if(a1 - y1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     y1=time.time()
                     str_variable = i["variable"]
                     topic= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, EstateVentilador)
                     status = result[0]
                     if status == 0:
                         print(f"Send Ventilador-Raspberry: `{EstateVentilador}` to topic `{topic}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic}")
            
            if(i["variableFullName"]=="Temperatura-Raspberry"):
                freq = i["variableSendFreq"]
                if(a1 - z1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     z1=time.time()
                     str_variable = i["variable"]
                     topic= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, Temp_Raspberry)
                     status = result[0]
                     if status == 0:
                         print(f"Send Temperatura-Raspberry: `{Temp_Raspberry}` to topic `{topic}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic}")
 

MaxVoltage15_1=0.0
MeanVoltage15_1=0.0
MinVoltage15_1=0.0
MaxCurrent15_1=0.0
MeanCurrent15_1=0.0
MinCurrent15_1=0.0
MaxActivePower_1=0.0
MeanActivePower_1=0.0
MinActivePower_1=0.0
MaxReactivePower_1=0.0
MeanReactivePower_1=0.0
MinReactivePower_1=0.0
MaxAparentPower_1=0.0
MeanAparentPower_1=0.0
MinAparentPower_1=0.0
MaxFPInductive_1=-0.99
MeanFPInductive_1=-0.99
MinFPInductive_1=-0.99
MaxFPReactive_1=0.99
MeanFPReactive_1=0.99
MinFPReactive_1=0.99
MaxFD_1=0.0
MeanFD_1=0.0
MinFD_1=0.0
MaxDAT_1=0.0
MeanDAT_1=0.0
MinDAT_1=0.0
Access_1 = 0
Volt15_1=[]
data15_1=[]
Current15_1=[]
ActivePower15_1=[]
ReactivePower15_1=[]
AparentPower15_1=[]
FP15_Reactive_1=[]
FP15_Inductive_1=[]
FDVoltage15_1=[]
FDCurrent15_1=[]
DAT15Voltage_1=[]
DAT15Current_1=[]

def Maximo15min_1(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_1
    global Volt15_1
    global data15_1
    global Current15_1
    global ActivePower15_1
    global ReactivePower15_1
    global AparentPower15_1
    global FP15_Reactive_1
    global FP15_Inductive_1
    global FDVoltage15_1
    global FDCurrent15_1
    global DAT15Voltage_1
    global DAT15Current_1
    global Access_1
    
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46): 
               if(Access_1 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_1 = 1
                    MaxVoltage15_1=max(Volt15_1)
                    MeanVoltage15_1=np.median(Volt15_1)
                    MinVoltage15_1=min(Volt15_1)
                    MaxCurrent15_1=max(Current15_1)
                    MeanCurrent15_1=np.median(Current15_1)
                    MinCurrent15_1=min(Current15_1)
                    MaxActivePower_1=max(ActivePower15_1)
                    MeanActivePower_1=np.median(ActivePower15_1)
                    MinActivePower_1=min(ActivePower15_1)
                    MaxReactivePower_1=max(ReactivePower15_1)
                    MeanReactivePower_1=np.median(ReactivePower15_1)
                    MinReactivePower_1=min(ReactivePower15_1)
                    MaxAparentPower_1=max(AparentPower15_1)
                    MeanAparentPower_1=np.median(AparentPower15_1)
                    MinAparentPower_1=min(AparentPower15_1)
                    if(len(FP15_Inductive_1)>0):
                           MaxFPInductive_1=max(FP15_Inductive_1)
                           MeanFPInductive_1=np.median(FP15_Inductive_1)
                           MinFPInductive_1=min(FP15_Inductive_1)
                    else:
                           MaxFPInductive_1=-0.99
                           MeanFPInductive_1=-0.99
                           MinFPInductive_1=-0.99
                    if(len(FP15_Reactive_1)>0):
                           MaxFPReactive_1=max(FP15_Reactive_1)
                           MeanFPReactive_1=np.median(FP15_Reactive_1)
                           MinFPReactive_1=min(FP15_Reactive_1)
                    else:
                           MaxFPReactive_1=0.99
                           MeanFPReactive_1=0.99
                           MinFPReactive_1=0.99
                    MaxFDVoltage_1=max(FDVoltage15_1)
                    MeanFDVoltage_1=np.median(FDVoltage15_1)
                    MinFDVoltage_1=min(FDVoltage15_1)
                    MaxFDCurrent_1=max(FDCurrent15_1)
                    MeanFDCurrent_1=np.median(FDCurrent15_1)
                    MinFDCurrent_1=min(FDCurrent15_1)
                    MaxDATVoltage_1=max(DAT15Voltage_1)
                    MeanDATVoltage_1=np.median(DAT15Voltage_1)
                    MinDATVoltage_1=min(DAT15Voltage_1)
                    MaxDATCurrent_1=max(DAT15Current_1)
                    MeanDATCurrent_1=np.median(DAT15Current_1)
                    MinDATCurrent_1=min(DAT15Current_1)
                    data15_1.insert(1,MaxVoltage15_1)
                    data15_1.insert(2,MeanVoltage15_1)
                    data15_1.insert(3,MinVoltage15_1)
                    data15_1.insert(4,MaxCurrent15_1)
                    data15_1.insert(5,MeanCurrent15_1)
                    data15_1.insert(6,MinCurrent15_1)
                    data15_1.insert(7,MaxActivePower_1)
                    data15_1.insert(8,MeanActivePower_1)
                    data15_1.insert(9,MinActivePower_1)
                    data15_1.insert(10,MaxReactivePower_1)
                    data15_1.insert(11,MeanReactivePower_1)
                    data15_1.insert(12,MinReactivePower_1)
                    data15_1.insert(13,MaxAparentPower_1)
                    data15_1.insert(14,MeanAparentPower_1)
                    data15_1.insert(15,MinAparentPower_1)
                    data15_1.insert(16,MaxFPInductive_1)
                    data15_1.insert(17,MeanFPInductive_1)
                    data15_1.insert(18,MinFPInductive_1)
                    data15_1.insert(19,MaxFPReactive_1)
                    data15_1.insert(20,MeanFPReactive_1)
                    data15_1.insert(21,MinFPReactive_1)
                    data15_1.insert(22,MaxFDVoltage_1)
                    data15_1.insert(23,MeanFDVoltage_1)
                    data15_1.insert(24,MinFDVoltage_1)
                    data15_1.insert(25,MaxFDCurrent_1)
                    data15_1.insert(26,MeanFDCurrent_1)
                    data15_1.insert(27,MinFDCurrent_1)
                    data15_1.insert(28,MaxDATVoltage_1)
                    data15_1.insert(29,MeanDATVoltage_1)
                    data15_1.insert(30,MinDATVoltage_1)
                    data15_1.insert(31,MaxDATCurrent_1)
                    data15_1.insert(32,MeanDATCurrent_1)
                    data15_1.insert(33,MinDATCurrent_1)
                    data15_1.insert(34,Energy)
                    data15_1.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet2 = workbook["Max Var 1"]
                    sheet2.append(list(data15_1))
                    print(f'Data 1: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_1=[]
                    Volt15_1=[]
                    Current15_1=[]
                    ActivePower15_1=[]
                    ReactivePower15_1=[]
                    AparentPower15_1=[]
                    FP15_Reactive_1=[]
                    FP15_Inductive_1=[]
                    FDVoltage15_1=[]
                    FDCurrent15_1=[]
                    DAT15Voltage_1=[]
                    DAT15Current_1=[]
               elif(Access_1==1):
                    #print("paso elif 2")
                    Volt15.append(Vrms)
                    Current15.append(Irms)
                    ActivePower15.append(ActivePower)
                    ReactivePower15.append(ReactivePower)
                    AparentPower15.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive.append(FP)
                    else: 
                          FP15_Inductive.append(FP)
                    FDVoltage15_1.append(FDVoltage)
                    FDCurrent15_1.append(FDCurrent)
                    DAT15Voltage_1.append(DATVoltage)
                    DAT15Current_1.append(DATCurrent)
              
    else:
        Volt15_1.append(Vrms)
        Current15_1.append(Irms)
        ActivePower15_1.append(ActivePower)
        ReactivePower15_1.append(ReactivePower)
        AparentPower15_1.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_1.append(FP)
        else: 
              FP15_Inductive_1.append(FP)
        FDVoltage15_1.append(FDVoltage)
        FDCurrent15_1.append(FDCurrent)
        DAT15Voltage_1.append(DATVoltage)
        DAT15Current_1.append(DATCurrent)
        Access_1 = 0
        
        if(len(Volt15_1)>2):
            indice=np.argmin(Volt15_1)
            Volt15_1.pop(indice)
            indice=np.argmin(Current15_1)
            Current15_1.pop(indice)
            indice=np.argmin(ActivePower15_1)
            ActivePower15_1.pop(indice)
            indice=np.argmin(ReactivePower15_1)
            ReactivePower15_1.pop(indice)
            indice=np.argmin(AparentPower15_1)
            AparentPower15_1.pop(indice)
            if(len(FP15_Reactive_1)>=2):
                indice=np.argmax(FP15_Reactive_1)
                FP15_Reactive_1.pop(indice)
            if(len(FP15_Inductive_1)>=2):
                indice=np.argmin(FP15_Inductive_1)
                FP15_Inductive_1.pop(indice)
            indice=np.argmin(FDVoltage15_1)
            FDVoltage15_1.pop(indice)
            indice=np.argmin(FDCurrent15_1)
            FDCurrent15_1.pop(indice)
            indice=np.argmin(DAT15Voltage_1)
            DAT15Voltage_1.pop(indice)
            indice=np.argmin(DAT15Current_1)
            DAT15Current_1.pop(indice)
        
        
Access_2 = 0
MaxVoltage15_2=0.0
MeanVoltage15_2=0.0
MinVoltage15_2=0.0
MaxCurrent15_2=0.0
MeanCurrent15_2=0.0
MinCurrent15_2=0.0
MaxActivePower_2=0.0
MeanActivePower_2=0.0
MinActivePower_2=0.0
MaxReactivePower_2=0.0
MeanReactivePower_2=0.0
MinReactivePower_2=0.0
MaxAparentPower_2=0.0
MeanAparentPower_2=0.0
MinAparentPower_2=0.0
MaxFPInductive_2=-0.99
MeanFPInductive_2=-0.99
MinFPInductive_2=-0.99
MaxFPReactive_2=0.99
MeanFPReactive_2=0.99
MinFPReactive_2=0.99
MaxFD_2=0.0
MeanFD_2=0.0
MinFD_2=0.0
MaxDAT_2=0.0
MeanDAT_2=0.0
MinDAT_2=0.0
Volt15_2=[]
data15_2=[]
Current15_2=[]
ActivePower15_2=[]
ReactivePower15_2=[]
AparentPower15_2=[]
FP15_Reactive_2=[]
FP15_Inductive_2=[]
FDVoltage15_2=[]
FDCurrent15_2=[]
DAT15Voltage_2=[]
DAT15Current_2=[]
def Maximo15min_2(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_2
    global Volt15_2
    global data15_2
    global Current15_2
    global ActivePower15_2
    global ReactivePower15_2
    global AparentPower15_2
    global FP15_Reactive_2
    global FP15_Inductive_2
    global FDVoltage15_2
    global FDCurrent15_2
    global DAT15Voltage_2
    global DAT15Current_2
    global Access_2
    
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_2 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_2 = 1
                    MaxVoltage15_2=max(Volt15_2)
                    MeanVoltage15_2=np.median(Volt15_2)
                    MinVoltage15_2=min(Volt15_2)
                    MaxCurrent15_2=max(Current15_2)
                    MeanCurrent15_2=np.median(Current15_2)
                    MinCurrent15_2=min(Current15_2)
                    MaxActivePower_2=max(ActivePower15_2)
                    MeanActivePower_2=np.median(ActivePower15_2)
                    MinActivePower_2=min(ActivePower15_2)
                    MaxReactivePower_2=max(ReactivePower15_2)
                    MeanReactivePower_2=np.median(ReactivePower15_2)
                    MinReactivePower_2=min(ReactivePower15_2)
                    MaxAparentPower_2=max(AparentPower15_2)
                    MeanAparentPower_2=np.median(AparentPower15_2)
                    MinAparentPower_2=min(AparentPower15_2)
                    if(len(FP15_Inductive_2)>0):
                           MaxFPInductive_2=max(FP15_Inductive_2)
                           MeanFPInductive_2=np.median(FP15_Inductive_2)
                           MinFPInductive_2=min(FP15_Inductive_2)
                    else:
                           MaxFPInductive_2=-0.99
                           MeanFPInductive_2=-0.99
                           MinFPInductive_2=-0.99
                    if(len(FP15_Reactive_2)>0):
                           MaxFPReactive_2=max(FP15_Reactive_2)
                           MeanFPReactive_2=np.median(FP15_Reactive_2)
                           MinFPReactive_2=min(FP15_Reactive_2)
                    else:
                           MaxFPReactive_2=0.99
                           MeanFPReactive_2=0.99
                           MinFPReactive_2=0.99
                    MaxFDVoltage_2=max(FDVoltage15_2)
                    MeanFDVoltage_2=np.median(FDVoltage15_2)
                    MinFDVoltage_2=min(FDVoltage15_2)
                    MaxFDCurrent_2=max(FDCurrent15_2)
                    MeanFDCurrent_2=np.median(FDCurrent15_2)
                    MinFDCurrent_2=min(FDCurrent15_2)
                    MaxDATVoltage_2=max(DAT15Voltage_2)
                    MeanDATVoltage_2=np.median(DAT15Voltage_2)
                    MinDATVoltage_2=min(DAT15Voltage_2)
                    MaxDATCurrent_2=max(DAT15Current_2)
                    MeanDATCurrent_2=np.median(DAT15Current_2)
                    MinDATCurrent_2=min(DAT15Current_2)
                    data15_2.insert(1,MaxVoltage15_2)
                    data15_2.insert(2,MeanVoltage15_2)
                    data15_2.insert(3,MinVoltage15_2)
                    data15_2.insert(4,MaxCurrent15_2)
                    data15_2.insert(5,MeanCurrent15_2)
                    data15_2.insert(6,MinCurrent15_2)
                    data15_2.insert(7,MaxActivePower_2)
                    data15_2.insert(8,MeanActivePower_2)
                    data15_2.insert(9,MinActivePower_2)
                    data15_2.insert(10,MaxReactivePower_2)
                    data15_2.insert(11,MeanReactivePower_2)
                    data15_2.insert(12,MinReactivePower_2)
                    data15_2.insert(13,MaxAparentPower_2)
                    data15_2.insert(14,MeanAparentPower_2)
                    data15_2.insert(15,MinAparentPower_2)
                    data15_2.insert(16,MaxFPInductive_2)
                    data15_2.insert(17,MeanFPInductive_2)
                    data15_2.insert(18,MinFPInductive_2)
                    data15_2.insert(19,MaxFPReactive_2)
                    data15_2.insert(20,MeanFPReactive_2)
                    data15_2.insert(21,MinFPReactive_2)
                    data15_2.insert(22,MaxFDVoltage_2)
                    data15_2.insert(23,MeanFDVoltage_2)
                    data15_2.insert(24,MinFDVoltage_2)
                    data15_2.insert(25,MaxFDCurrent_2)
                    data15_2.insert(26,MeanFDCurrent_2)
                    data15_2.insert(27,MinFDCurrent_2)
                    data15_2.insert(28,MaxDATVoltage_2)
                    data15_2.insert(29,MeanDATVoltage_2)
                    data15_2.insert(30,MinDATVoltage_2)
                    data15_2.insert(31,MaxDATCurrent_2)
                    data15_2.insert(32,MeanDATCurrent_2)
                    data15_2.insert(33,MinDATCurrent_2)
                    data15_2.insert(34,Energy)
                    data15_2.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet3 = workbook["Max Var 2"]
                    sheet3.append(list(data15_2))
                    print(f'Data 2: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_2=[]
                    Volt15_2=[]
                    Current15_2=[]
                    ActivePower15_2=[]
                    ReactivePower15_2=[]
                    AparentPower15_2=[]
                    FP15_Reactive_2=[]
                    FP15_Inductive_2=[]
                    FDVoltage15_2=[]
                    FDCurrent15_2=[]
                    DAT15Voltage_2=[]
                    DAT15Current_2=[]
               elif(Access_2==1):
                    #print("paso elif 2")
                    Volt15_2.append(Vrms)
                    Current15_2.append(Irms)
                    ActivePower15_2.append(ActivePower)
                    ReactivePower15_2.append(ReactivePower)
                    AparentPower15_2.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_2.append(FP)
                    else: 
                          FP15_Inductive_2.append(FP)
                    FDVoltage15_2.append(FDVoltage)
                    FDCurrent15_2.append(FDCurrent)
                    DAT15Voltage_2.append(DATVoltage)
                    DAT15Current_2.append(DATCurrent)
              
    else:
        Volt15_2.append(Vrms)
        Current15_2.append(Irms)
        ActivePower15_2.append(ActivePower)
        ReactivePower15_2.append(ReactivePower)
        AparentPower15_2.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_2.append(FP)
        else: 
              FP15_Inductive_2.append(FP)
        FDVoltage15_2.append(FDVoltage)
        FDCurrent15_2.append(FDCurrent)
        DAT15Voltage_2.append(DATVoltage)
        DAT15Current_2.append(DATCurrent)
        Access_2 = 0
        
        if(len(Volt15_2)>2):
            indice=np.argmin(Volt15_2)
            Volt15_2.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_2)
            Current15_2.pop(indice)
            indice=np.argmin(ActivePower15_2)
            ActivePower15_2.pop(indice)
            indice=np.argmin(ReactivePower15_2)
            ReactivePower15_2.pop(indice)
            indice=np.argmin(AparentPower15_2)
            AparentPower15_2.pop(indice)
            if(len(FP15_Reactive_2)>=2):
                indice=np.argmax(FP15_Reactive_2)
                FP15_Reactive_2.pop(indice)
            if(len(FP15_Inductive_2)>=2):
                indice=np.argmin(FP15_Inductive_2)
                FP15_Inductive_2.pop(indice)
            indice=np.argmin(FDVoltage15_2)
            FDVoltage15_2.pop(indice)
            indice=np.argmin(FDCurrent15_2)
            FDCurrent15_2.pop(indice)
            indice=np.argmin(DAT15Voltage_2)
            DAT15Voltage_2.pop(indice)
            indice=np.argmin(DAT15Current_2)
            DAT15Current_2.pop(indice)
    
Access_3 = 0
MaxVoltage15_3=0.0
MeanVoltage15_3=0.0
MinVoltage15_3=0.0
MaxCurrent15_3=0.0
MeanCurrent15_3=0.0
MinCurrent15_3=0.0
MaxActivePower_3=0.0
MeanActivePower_3=0.0
MinActivePower_3=0.0
MaxReactivePower_3=0.0
MeanReactivePower_3=0.0
MinReactivePower_3=0.0
MaxAparentPower_3=0.0
MeanAparentPower_3=0.0
MinAparentPower_3=0.0
MaxFPInductive_3=-0.99
MeanFPInductive_3=-0.99
MinFPInductive_3=-0.99
MaxFPReactive_3=0.99
MeanFPReactive_3=0.99
MinFPReactive_3=0.99
MaxFD_3=0.0
MeanFD_3=0.0
MinFD_3=0.0
MaxDAT_3=0.0
MeanDAT_3=0.0
MinDAT_3=0.0
Volt15_3=[]
data15_3=[]
Current15_3=[]
ActivePower15_3=[]
ReactivePower15_3=[]
AparentPower15_3=[]
FP15_Reactive_3=[]
FP15_Inductive_3=[]
FDVoltage15_3=[]
FDCurrent15_3=[]
DAT15Voltage_3=[]
DAT15Current_3=[]
def Maximo15min_3(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_3
    global Volt15_3
    global data15_3
    global Current15_3
    global ActivePower15_3
    global ReactivePower15_3
    global AparentPower15_3
    global FP15_Reactive_3
    global FP15_Inductive_3
    global FDVoltage15_3
    global FDCurrent15_3
    global DAT15Voltage_3
    global DAT15Current_3
    global Access_3
    
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_3 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_3 = 1
                    MaxVoltage15_3=max(Volt15_3)
                    MeanVoltage15_3=np.median(Volt15_3)
                    MinVoltage15_3=min(Volt15_3)
                    MaxCurrent15_3=max(Current15_3)
                    MeanCurrent15_3=np.median(Current15_3)
                    MinCurrent15_3=min(Current15_3)
                    MaxActivePower_3=max(ActivePower15_3)
                    MeanActivePower_3=np.median(ActivePower15_3)
                    MinActivePower_3=min(ActivePower15_3)
                    MaxReactivePower_3=max(ReactivePower15_3)
                    MeanReactivePower_3=np.median(ReactivePower15_3)
                    MinReactivePower_3=min(ReactivePower15_3)
                    MaxAparentPower_3=max(AparentPower15_3)
                    MeanAparentPower_3=np.median(AparentPower15_3)
                    MinAparentPower_3=min(AparentPower15_3)
                    if(len(FP15_Inductive_3)>0):
                           MaxFPInductive_3=max(FP15_Inductive_3)
                           MeanFPInductive_3=np.median(FP15_Inductive_3)
                           MinFPInductive_3=min(FP15_Inductive_3)
                    else:
                           MaxFPInductive_3=-0.99
                           MeanFPInductive_3=-0.99
                           MinFPInductive_3=-0.99
                    if(len(FP15_Reactive_3)>0):
                           MaxFPReactive_3=max(FP15_Reactive_3)
                           MeanFPReactive_3=np.median(FP15_Reactive_3)
                           MinFPReactive_3=min(FP15_Reactive_3)
                    else:
                           MaxFPReactive_3=0.99
                           MeanFPReactive_3=0.99
                           MinFPReactive_3=0.99
                    MaxFDVoltage_3=max(FDVoltage15_3)
                    MeanFDVoltage_3=np.median(FDVoltage15_3)
                    MinFDVoltage_3=min(FDVoltage15_3)
                    MaxFDCurrent_3=max(FDCurrent15_3)
                    MeanFDCurrent_3=np.median(FDCurrent15_3)
                    MinFDCurrent_3=min(FDCurrent15_3)
                    MaxDATVoltage_3=max(DAT15Voltage_3)
                    MeanDATVoltage_3=np.median(DAT15Voltage_3)
                    MinDATVoltage_3=min(DAT15Voltage_3)
                    MaxDATCurrent_3=max(DAT15Current_3)
                    MeanDATCurrent_3=np.median(DAT15Current_3)
                    MinDATCurrent_3=min(DAT15Current_3)
                    data15_3.insert(1,MaxVoltage15_3)
                    data15_3.insert(2,MeanVoltage15_3)
                    data15_3.insert(3,MinVoltage15_3)
                    data15_3.insert(4,MaxCurrent15_3)
                    data15_3.insert(5,MeanCurrent15_3)
                    data15_3.insert(6,MinCurrent15_3)
                    data15_3.insert(7,MaxActivePower_3)
                    data15_3.insert(8,MeanActivePower_3)
                    data15_3.insert(9,MinActivePower_3)
                    data15_3.insert(10,MaxReactivePower_3)
                    data15_3.insert(11,MeanReactivePower_3)
                    data15_3.insert(12,MinReactivePower_3)
                    data15_3.insert(13,MaxAparentPower_3)
                    data15_3.insert(14,MeanAparentPower_3)
                    data15_3.insert(15,MinAparentPower_3)
                    data15_3.insert(16,MaxFPInductive_3)
                    data15_3.insert(17,MeanFPInductive_3)
                    data15_3.insert(18,MinFPInductive_3)
                    data15_3.insert(19,MaxFPReactive_3)
                    data15_3.insert(20,MeanFPReactive_3)
                    data15_3.insert(21,MinFPReactive_3)
                    data15_3.insert(22,MaxFDVoltage_3)
                    data15_3.insert(23,MeanFDVoltage_3)
                    data15_3.insert(24,MinFDVoltage_3)
                    data15_3.insert(25,MaxFDCurrent_3)
                    data15_3.insert(26,MeanFDCurrent_3)
                    data15_3.insert(27,MinFDCurrent_3)
                    data15_3.insert(28,MaxDATVoltage_3)
                    data15_3.insert(29,MeanDATVoltage_3)
                    data15_3.insert(30,MinDATVoltage_3)
                    data15_3.insert(31,MaxDATCurrent_3)
                    data15_3.insert(32,MeanDATCurrent_3)
                    data15_3.insert(33,MinDATCurrent_3)
                    data15_3.insert(34,Energy)
                    data15_3.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet4 = workbook["Max Var 3"]
                    sheet4.append(list(data15_3))
                    print(f'Data 3: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_3=[]
                    Volt15_3=[]
                    Current15_3=[]
                    ActivePower15_3=[]
                    ReactivePower15_3=[]
                    AparentPower15_3=[]
                    FP15_Reactive_3=[]
                    FP15_Inductive_3=[]
                    FDVoltage15_3=[]
                    FDCurrent15_3=[]
                    DAT15Voltage_3=[]
                    DAT15Current_3=[]
               elif(Access_3==1):
                    #print("paso elif 2")
                    Volt15_3.append(Vrms)
                    Current15_3.append(Irms)
                    ActivePower15_3.append(ActivePower)
                    ReactivePower15_3.append(ReactivePower)
                    AparentPower15_3.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_3.append(FP)
                    else: 
                          FP15_Inductive_3.append(FP)
                    FDVoltage15_3.append(FDVoltage)
                    FDCurrent15_3.append(FDCurrent)
                    DAT15Voltage_3.append(DATVoltage)
                    DAT15Current_3.append(DATCurrent)
              
    else:
        Volt15_3.append(Vrms)
        Current15_3.append(Irms)
        ActivePower15_3.append(ActivePower)
        ReactivePower15_3.append(ReactivePower)
        AparentPower15_3.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_3.append(FP)
        else: 
              FP15_Inductive_3.append(FP)
        FDVoltage15_3.append(FDVoltage)
        FDCurrent15_3.append(FDCurrent)
        DAT15Voltage_3.append(DATVoltage)
        DAT15Current_3.append(DATCurrent)
        Access_3 = 0
        
        if(len(Volt15_3)>2):
            indice=np.argmin(Volt15_3)
            Volt15_3.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_3)
            Current15_3.pop(indice)
            indice=np.argmin(ActivePower15_3)
            ActivePower15_3.pop(indice)
            indice=np.argmin(ReactivePower15_3)
            ReactivePower15_3.pop(indice)
            indice=np.argmin(AparentPower15_3)
            AparentPower15_3.pop(indice)
            if(len(FP15_Reactive_3)>=2):
                indice=np.argmax(FP15_Reactive_3)
                FP15_Reactive_3.pop(indice)
            if(len(FP15_Inductive_3)>=2):
                indice=np.argmin(FP15_Inductive_3)
                FP15_Inductive_3.pop(indice)
            indice=np.argmin(FDVoltage15_3)
            FDVoltage15_3.pop(indice)
            indice=np.argmin(FDCurrent15_3)
            FDCurrent15_3.pop(indice)
            indice=np.argmin(DAT15Voltage_3)
            DAT15Voltage_3.pop(indice)
            indice=np.argmin(DAT15Current_3)
            DAT15Current_3.pop(indice)

Access_4 = 0
MaxVoltage15_4=0.0
MeanVoltage15_4=0.0
MinVoltage15_4=0.0
MaxCurrent15_4=0.0
MeanCurrent15_4=0.0
MinCurrent15_4=0.0
MaxActivePower_4=0.0
MeanActivePower_4=0.0
MinActivePower_4=0.0
MaxReactivePower_4=0.0
MeanReactivePower_4=0.0
MinReactivePower_4=0.0
MaxAparentPower_4=0.0
MeanAparentPower_4=0.0
MinAparentPower_4=0.0
MaxFPInductive_4=-0.99
MeanFPInductive_4=-0.99
MinFPInductive_4=-0.99
MaxFPReactive_4=0.99
MeanFPReactive_4=0.99
MinFPReactive_4=0.99
MaxFD_4=0.0
MeanFD_4=0.0
MinFD_4=0.0
MaxDAT_4=0.0
MeanDAT_4=0.0
MinDAT_4=0.0
Volt15_4=[]
data15_4=[]
Current15_4=[]
ActivePower15_4=[]
ReactivePower15_4=[]
AparentPower15_4=[]
FP15_Reactive_4=[]
FP15_Inductive_4=[]
FDVoltage15_4=[]
FDCurrent15_4=[]
DAT15Voltage_4=[]
DAT15Current_4=[]
def Maximo15min_4(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_4
    global Volt15_4
    global data15_4
    global Current15_4
    global ActivePower15_4
    global ReactivePower15_4
    global AparentPower15_4
    global FP15_Reactive_4
    global FP15_Inductive_4
    global FDVoltage15_4
    global FDCurrent15_4
    global DAT15Voltage_4
    global DAT15Current_4
    global Access_4
    
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_4 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_4 = 1
                    MaxVoltage15_4=max(Volt15_4)
                    MeanVoltage15_4=np.median(Volt15_4)
                    MinVoltage15_4=min(Volt15_4)
                    MaxCurrent15_4=max(Current15_4)
                    MeanCurrent15_4=np.median(Current15_4)
                    MinCurrent15_4=min(Current15_4)
                    MaxActivePower_4=max(ActivePower15_4)
                    MeanActivePower_4=np.median(ActivePower15_4)
                    MinActivePower_4=min(ActivePower15_4)
                    MaxReactivePower_4=max(ReactivePower15_4)
                    MeanReactivePower_4=np.median(ReactivePower15_4)
                    MinReactivePower_4=min(ReactivePower15_4)
                    MaxAparentPower_4=max(AparentPower15_4)
                    MeanAparentPower_4=np.median(AparentPower15_4)
                    MinAparentPower_4=min(AparentPower15_4)
                    if(len(FP15_Inductive_4)>0):
                           MaxFPInductive_4=max(FP15_Inductive_4)
                           MeanFPInductive_4=np.median(FP15_Inductive_4)
                           MinFPInductive_4=min(FP15_Inductive_4)
                    else:
                           MaxFPInductive_4=-0.99
                           MeanFPInductive_4=-0.99
                           MinFPInductive_4=-0.99
                    if(len(FP15_Reactive_4)>0):
                           MaxFPReactive_4=max(FP15_Reactive_4)
                           MeanFPReactive_4=np.median(FP15_Reactive_4)
                           MinFPReactive_4=min(FP15_Reactive_4)
                    else:
                           MaxFPReactive_4=0.99
                           MeanFPReactive_4=0.99
                           MinFPReactive_4=0.99
                    MaxFDVoltage_4=max(FDVoltage15_4)
                    MeanFDVoltage_4=np.median(FDVoltage15_4)
                    MinFDVoltage_4=min(FDVoltage15_4)
                    MaxFDCurrent_4=max(FDCurrent15_4)
                    MeanFDCurrent_4=np.median(FDCurrent15_4)
                    MinFDCurrent_4=min(FDCurrent15_4)
                    MaxDATVoltage_4=max(DAT15Voltage_4)
                    MeanDATVoltage_4=np.median(DAT15Voltage_4)
                    MinDATVoltage_4=min(DAT15Voltage_4)
                    MaxDATCurrent_4=max(DAT15Current_4)
                    MeanDATCurrent_4=np.median(DAT15Current_4)
                    MinDATCurrent_4=min(DAT15Current_4)
                    data15_4.insert(1,MaxVoltage15_4)
                    data15_4.insert(2,MeanVoltage15_4)
                    data15_4.insert(3,MinVoltage15_4)
                    data15_4.insert(4,MaxCurrent15_4)
                    data15_4.insert(5,MeanCurrent15_4)
                    data15_4.insert(6,MinCurrent15_4)
                    data15_4.insert(7,MaxActivePower_4)
                    data15_4.insert(8,MeanActivePower_4)
                    data15_4.insert(9,MinActivePower_4)
                    data15_4.insert(10,MaxReactivePower_4)
                    data15_4.insert(11,MeanReactivePower_4)
                    data15_4.insert(12,MinReactivePower_4)
                    data15_4.insert(13,MaxAparentPower_4)
                    data15_4.insert(14,MeanAparentPower_4)
                    data15_4.insert(15,MinAparentPower_4)
                    data15_4.insert(16,MaxFPInductive_4)
                    data15_4.insert(17,MeanFPInductive_4)
                    data15_4.insert(18,MinFPInductive_4)
                    data15_4.insert(19,MaxFPReactive_4)
                    data15_4.insert(20,MeanFPReactive_4)
                    data15_4.insert(21,MinFPReactive_4)
                    data15_4.insert(22,MaxFDVoltage_4)
                    data15_4.insert(23,MeanFDVoltage_4)
                    data15_4.insert(24,MinFDVoltage_4)
                    data15_4.insert(25,MaxFDCurrent_4)
                    data15_4.insert(26,MeanFDCurrent_4)
                    data15_4.insert(27,MinFDCurrent_4)
                    data15_4.insert(28,MaxDATVoltage_4)
                    data15_4.insert(29,MeanDATVoltage_4)
                    data15_4.insert(30,MinDATVoltage_4)
                    data15_4.insert(31,MaxDATCurrent_4)
                    data15_4.insert(32,MeanDATCurrent_4)
                    data15_4.insert(33,MinDATCurrent_4)
                    data15_4.insert(34,Energy)
                    data15_4.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet5 = workbook["Max Var 4"]
                    sheet5.append(list(data15_4))
                    print(f'Data 4: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_4=[]
                    Volt15_4=[]
                    Current15_4=[]
                    ActivePower15_4=[]
                    ReactivePower15_4=[]
                    AparentPower15_4=[]
                    FP15_Reactive_4=[]
                    FP15_Inductive_4=[]
                    FDVoltage15_4=[]
                    FDCurrent15_4=[]
                    DAT15Voltage_4=[]
                    DAT15Current_4=[]
               elif(Access_4==1):
                    #print("paso elif 2")
                    Volt15_4.append(Vrms)
                    Current15_4.append(Irms)
                    ActivePower15_4.append(ActivePower)
                    ReactivePower15_4.append(ReactivePower)
                    AparentPower15_4.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_4.append(FP)
                    else: 
                          FP15_Inductive_4.append(FP)
                    FDVoltage15_4.append(FDVoltage)
                    FDCurrent15_4.append(FDCurrent)
                    DAT15Voltage_4.append(DATVoltage)
                    DAT15Current_4.append(DATCurrent)      
    else:
        Volt15_4.append(Vrms)
        Current15_4.append(Irms)
        ActivePower15_4.append(ActivePower)
        ReactivePower15_4.append(ReactivePower)
        AparentPower15_4.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_4.append(FP)
        else: 
              FP15_Inductive_4.append(FP)
        FDVoltage15_4.append(FDVoltage)
        FDCurrent15_4.append(FDCurrent)
        DAT15Voltage_4.append(DATVoltage)
        DAT15Current_4.append(DATCurrent)
        Access_4 = 0
        
        if(len(Volt15_4)>2):
            indice=np.argmin(Volt15_4)
            Volt15_4.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_4)
            Current15_4.pop(indice)
            indice=np.argmin(ActivePower15_4)
            ActivePower15_4.pop(indice)
            indice=np.argmin(ReactivePower15_4)
            ReactivePower15_4.pop(indice)
            indice=np.argmin(AparentPower15_4)
            AparentPower15_4.pop(indice)
            if(len(FP15_Reactive_4)>=2):
                indice=np.argmax(FP15_Reactive_4)
                FP15_Reactive_4.pop(indice)
            if(len(FP15_Inductive_4)>=2):
                indice=np.argmin(FP15_Inductive_4)
                FP15_Inductive_4.pop(indice)
            indice=np.argmin(FDVoltage15_4)
            FDVoltage15_4pop(indice)
            indice=np.argmin(FDCurrent15_4)
            FDCurrent15_4.pop(indice)
            indice=np.argmin(DAT15Voltage_4)
            DAT15Voltage_4.pop(indice)
            indice=np.argmin(DAT15Current_4)
            DAT15Current_4.pop(indice)

Access_5 = 0
MaxVoltage15_5=0.0
MeanVoltage15_5=0.0
MinVoltage15_5=0.0
MaxCurrent15_5=0.0
MeanCurrent15_5=0.0
MinCurrent15_5=0.0
MaxActivePower_5=0.0
MeanActivePower_5=0.0
MinActivePower_5=0.0
MaxReactivePower_5=0.0
MeanReactivePower_5=0.0
MinReactivePower_5=0.0
MaxAparentPower_5=0.0
MeanAparentPower_5=0.0
MinAparentPower_5=0.0
MaxFPInductive_5=-0.99
MeanFPInductive_5=-0.99
MinFPInductive_5=-0.99
MaxFPReactive_5=0.99
MeanFPReactive_5=0.99
MinFPReactive_5=0.99
MaxFD_5=0.0
MeanFD_5=0.0
MinFD_5=0.0
MaxDAT_5=0.0
MeanDAT_5=0.0
MinDAT_5=0.0
Volt15_5=[]
data15_5=[]
Current15_5=[]
ActivePower15_5=[]
ReactivePower15_5=[]
AparentPower15_5=[]
FP15_Reactive_5=[]
FP15_Inductive_5=[]
FDVoltage15_5=[]
FDCurrent15_5=[]
DAT15Voltage_5=[]
DAT15Current_5=[]
def Maximo15min_5(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_5
    global Volt15_5
    global data15_5
    global Current15_5
    global ActivePower15_5
    global ReactivePower15_5
    global AparentPower15_5
    global FP15_Reactive_5
    global FP15_Inductive_5
    global FDVoltage15_5
    global FDCurrent15_5
    global DAT15Voltage_5
    global DAT15Current_5
    global Access_5
    
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_5 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_5 = 1
                    MaxVoltage15_5=max(Volt15_5)
                    MeanVoltage15_5=np.median(Volt15_5)
                    MinVoltage15_5=min(Volt15_5)
                    MaxCurrent15_5=max(Current15_5)
                    MeanCurrent15_5=np.median(Current15_5)
                    MinCurrent15_5=min(Current15_5)
                    MaxActivePower_5=max(ActivePower15_5)
                    MeanActivePower_5=np.median(ActivePower15_5)
                    MinActivePower_5=min(ActivePower15_5)
                    MaxReactivePower_5=max(ReactivePower15_5)
                    MeanReactivePower_5=np.median(ReactivePower15_5)
                    MinReactivePower_5=min(ReactivePower15_5)
                    MaxAparentPower_5=max(AparentPower15_5)
                    MeanAparentPower_5=np.median(AparentPower15_5)
                    MinAparentPower_5=min(AparentPower15_5)
                    if(len(FP15_Inductive_5)>0):
                           MaxFPInductive_5=max(FP15_Inductive_5)
                           MeanFPInductive_5=np.median(FP15_Inductive_5)
                           MinFPInductive_5=min(FP15_Inductive_5)
                    else:
                           MaxFPInductive_5=-0.99
                           MeanFPInductive_5=-0.99
                           MinFPInductive_5=-0.99
                    if(len(FP15_Reactive_5)>0):
                           MaxFPReactive_5=max(FP15_Reactive_5)
                           MeanFPReactive_5=np.median(FP15_Reactive_5)
                           MinFPReactive_5=min(FP15_Reactive_5)
                    else:
                           MaxFPReactive_5=0.99
                           MeanFPReactive_5=0.99
                           MinFPReactive_5=0.99
                    MaxFDVoltage_5=max(FDVoltage15_5)
                    MeanFDVoltage_5=np.median(FDVoltage15_5)
                    MinFDVoltage_5=min(FDVoltage15_5)
                    MaxFDCurrent_5=max(FDCurrent15_5)
                    MeanFDCurrent_5=np.median(FDCurrent15_5)
                    MinFDCurrent_5=min(FDCurrent15_5)
                    MaxDATVoltage_5=max(DAT15Voltage_5)
                    MeanDATVoltage_5=np.median(DAT15Voltage_5)
                    MinDATVoltage_5=min(DAT15Voltage_5)
                    MaxDATCurrent_5=max(DAT15Current_5)
                    MeanDATCurrent_5=np.median(DAT15Current_5)
                    MinDATCurrent_5=min(DAT15Current_5)
                    data15_5.insert(1,MaxVoltage15_5)
                    data15_5.insert(2,MeanVoltage15_5)
                    data15_5.insert(3,MinVoltage15_5)
                    data15_5.insert(4,MaxCurrent15_5)
                    data15_5.insert(5,MeanCurrent15_5)
                    data15_5.insert(6,MinCurrent15_5)
                    data15_5.insert(7,MaxActivePower_5)
                    data15_5.insert(8,MeanActivePower_5)
                    data15_5.insert(9,MinActivePower_5)
                    data15_5.insert(10,MaxReactivePower_5)
                    data15_5.insert(11,MeanReactivePower_5)
                    data15_5.insert(12,MinReactivePower_5)
                    data15_5.insert(13,MaxAparentPower_5)
                    data15_5.insert(14,MeanAparentPower_5)
                    data15_5.insert(15,MinAparentPower_5)
                    data15_5.insert(16,MaxFPInductive_5)
                    data15_5.insert(17,MeanFPInductive_5)
                    data15_5.insert(18,MinFPInductive_5)
                    data15_5.insert(19,MaxFPReactive_5)
                    data15_5.insert(20,MeanFPReactive_5)
                    data15_5.insert(21,MinFPReactive_5)
                    data15_5.insert(22,MaxFDVoltage_5)
                    data15_5.insert(23,MeanFDVoltage_5)
                    data15_5.insert(24,MinFDVoltage_5)
                    data15_5.insert(25,MaxFDCurrent_5)
                    data15_5.insert(26,MeanFDCurrent_5)
                    data15_5.insert(27,MinFDCurrent_5)
                    data15_5.insert(28,MaxDATVoltage_5)
                    data15_5.insert(29,MeanDATVoltage_5)
                    data15_5.insert(30,MinDATVoltage_5)
                    data15_5.insert(31,MaxDATCurrent_5)
                    data15_5.insert(32,MeanDATCurrent_5)
                    data15_5.insert(33,MinDATCurrent_5)
                    data15_5.insert(34,Energy)
                    data15_5.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet6 = workbook["Max Var 5"]
                    sheet6.append(list(data15_5))
                    print(f'Data 5: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_5=[]
                    Volt15_5=[]
                    Current15_5=[]
                    ActivePower15_5=[]
                    ReactivePower15_5=[]
                    AparentPower15_5=[]
                    FP15_Reactive_5=[]
                    FP15_Inductive_5=[]
                    FDVoltage15_5=[]
                    FDCurrent15_5=[]
                    DAT15Voltage_5=[]
                    DAT15Current_5=[]
               elif(Access_5==1):
                    #print("paso elif 2")
                    Volt15_5.append(Vrms)
                    Current15_5.append(Irms)
                    ActivePower15_5.append(ActivePower)
                    ReactivePower15_5.append(ReactivePower)
                    AparentPower15_5.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_5.append(FP)
                    else: 
                          FP15_Inductive_5.append(FP)
                    FDVoltage15_5.append(FDVoltage)
                    FDCurrent15_5.append(FDCurrent)
                    DAT15Voltage_5.append(DATVoltage)
                    DAT15Current_5.append(DATCurrent)
              
    else:
        Volt15_5.append(Vrms)
        Current15_5.append(Irms)
        ActivePower15_5.append(ActivePower)
        ReactivePower15_5.append(ReactivePower)
        AparentPower15_5.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_5.append(FP)
        else: 
              FP15_Inductive_5.append(FP)
        FDVoltage15_5.append(FDVoltage)
        FDCurrent15_5.append(FDCurrent)
        DAT15Voltage_5.append(DATVoltage)
        DAT15Current_5.append(DATCurrent)
        Access_5 = 8
        
        if(len(Volt15_5)>2):
            indice=np.argmin(Volt15_5)
            Volt15_5.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_5)
            Current15_5.pop(indice)
            indice=np.argmin(ActivePower15_5)
            ActivePower15_5.pop(indice)
            indice=np.argmin(ReactivePower15_5)
            ReactivePower15_5.pop(indice)
            indice=np.argmin(AparentPower15_5)
            AparentPower15_5.pop(indice)
            if(len(FP15_Reactive_5)>=2):
                indice=np.argmax(FP15_Reactive_5)
                FP15_Reactive_5.pop(indice)
            if(len(FP15_Inductive_5)>=2):
                indice=np.argmin(FP15_Inductive_5)
                FP15_Inductive_5.pop(indice)
            indice=np.argmin(FDVoltage15_5)
            FDVoltage15_5.pop(indice)
            indice=np.argmin(FDCurrent15_5)
            FDCurrent15_5.pop(indice)
            indice=np.argmin(DAT15Voltage_5)
            DAT15Voltage_5.pop(indice)
            indice=np.argmin(DAT15Current_5)
            DAT15Current_5.pop(indice)

Access_7 = 0
MaxVoltage15_7=0.0
MeanVoltage15_7=0.0
MinVoltage15_7=0.0
MaxCurrent15_7=0.0
MeanCurrent15_7=0.0
MinCurrent15_7=0.0
MaxActivePower_7=0.0
MeanActivePower_7=0.0
MinActivePower_7=0.0
MaxReactivePower_7=0.0
MeanReactivePower_7=0.7
MinReactivePower_7=0.0
MaxAparentPower_7=0.0
MeanAparentPower_7=0.0
MinAparentPower_7=0.0
MaxFPInductive_7=-0.99
MeanFPInductive_7=-0.99
MinFPInductive_7=-0.99
MaxFPReactive_7=0.99
MeanFPReactive_7=0.99
MinFPReactive_7=0.99
MaxFD_7=0.0
MeanFD_7=0.0
MinFD_7=0.0
MaxDAT_7=0.0
MeanDAT_7=0.0
MinDAT_7=0.0
Volt15_7=[]
data15_7=[]
Current15_7=[]
ActivePower15_7=[]
ReactivePower15_7=[]
AparentPower15_7=[]
FP15_Reactive_7=[]
FP15_Inductive_7=[]
FDVoltage15_7=[]
FDCurrent15_7=[]
DAT15Voltage_7=[]
DAT15Current_7=[]
def Maximo15min_7(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_7
    global Volt15_7
    global data15_7
    global Current15_7
    global ActivePower15_7
    global ReactivePower15_7
    global AparentPower15_7
    global FP15_Reactive_7
    global FP15_Inductive_7
    global FDVoltage15_7
    global FDCurrent15_7
    global DAT15Voltage_7
    global DAT15Current_7
    global Access_7
    
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_7 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_7 = 1
                    MaxVoltage15_7=max(Volt15_7)
                    MeanVoltage15_7=np.median(Volt15_7)
                    MinVoltage15_7=min(Volt15_7)
                    MaxCurrent15_7=max(Current15_7)
                    MeanCurrent15_7=np.median(Current15_7)
                    MinCurrent15_7=min(Current15_7)
                    MaxActivePower_7=max(ActivePower15_7)
                    MeanActivePower_7=np.median(ActivePower15_7)
                    MinActivePower_7=min(ActivePower15_7)
                    MaxReactivePower_7=max(ReactivePower15_7)
                    MeanReactivePower_7=np.median(ReactivePower15_7)
                    MinReactivePower_7=min(ReactivePower15_7)
                    MaxAparentPower_7=max(AparentPower15_7)
                    MeanAparentPower_7=np.median(AparentPower15_7)
                    MinAparentPower_7=min(AparentPower15_7)
                    if(len(FP15_Inductive_7)>0):
                           MaxFPInductive_7=max(FP15_Inductive_7)
                           MeanFPInductive_7=np.median(FP15_Inductive_7)
                           MinFPInductive_7=min(FP15_Inductive_7)
                    else:
                           MaxFPInductive_7=-0.99
                           MeanFPInductive_7=-0.99
                           MinFPInductive_7=-0.99
                    if(len(FP15_Reactive_7)>0):
                           MaxFPReactive_7=max(FP15_Reactive_7)
                           MeanFPReactive_7=np.median(FP15_Reactive_7)
                           MinFPReactive_7=min(FP15_Reactive_7)
                    else:
                           MaxFPReactive_7=0.99
                           MeanFPReactive_7=0.99
                           MinFPReactive_7=0.99
                    MaxFDVoltage_7=max(FDVoltage15_7)
                    MeanFDVoltage_7=np.median(FDVoltage15_7)
                    MinFDVoltage_7=min(FDVoltage15_7)
                    MaxFDCurrent_7=max(FDCurrent15_7)
                    MeanFDCurrent_7=np.median(FDCurrent15_7)
                    MinFDCurrent_7=min(FDCurrent15_7)
                    MaxDATVoltage_7=max(DAT15Volt3ge_7)
                    MeanDATVoltage_7=np.median(DAT15Voltage_7)
                    MinDATVoltage_7=min(DAT15Voltage_7)
                    MaxDATCurrent_7=max(DAT15Current_7)
                    MeanDATCurrent_7=np.median(DAT15Current_7)
                    MinDATCurrent_7=min(DAT15Current_7)
                    data15_7.insert(1,MaxVoltage15_7)
                    data15_7.insert(2,MeanVoltage15_7)
                    data15_7.insert(3,MinVoltage15_7)
                    data15_7.insert(4,MaxCurrent15_7)
                    data15_7.insert(5,MeanCurrent15_7)
                    data15_7.insert(6,MinCurrent15_7)
                    data15_7.insert(7,MaxActivePower_7)
                    data15_7.insert(8,MeanActivePower_7)
                    data15_7.insert(9,MinActivePower_7)
                    data15_7.insert(10,MaxReactivePower_7)
                    data15_7.insert(11,MeanReactivePower_7)
                    data15_7.insert(12,MinReactivePower_7)
                    data15_7.insert(13,MaxAparentPower_7)
                    data15_7.insert(14,MeanAparentPower_7)
                    data15_7.insert(15,MinAparentPower_7)
                    data15_7.insert(16,MaxFPInductive_7)
                    data15_7.insert(17,MeanFPInductive_7)
                    data15_7.insert(18,MinFPInductive_7)
                    data15_7.insert(19,MaxFPReactive_7)
                    data15_7.insert(20,MeanFPReactive_7)
                    data15_7.insert(21,MinFPReactive_7)
                    data15_7.insert(22,MaxFDVoltage_7)
                    data15_7.insert(23,MeanFDVoltage_7)
                    data15_7.insert(24,MinFDVoltage_7)
                    data15_7.insert(25,MaxFDCurrent_7)
                    data15_7.insert(26,MeanFDCurrent_7)
                    data15_7.insert(27,MinFDCurrent_7)
                    data15_7.insert(28,MaxDATVoltage_7)
                    data15_7.insert(29,MeanDATVoltage_7)
                    data15_7.insert(30,MinDATVoltage_7)
                    data15_7.insert(31,MaxDATCurrent_7)
                    data15_7.insert(32,MeanDATCurrent_7)
                    data15_7.insert(33,MinDATCurrent_7)
                    data15_7.insert(34,Energy)
                    data15_7.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet8 = workbook["Max Var 7"]
                    sheet8.append(list(data15_7))
                    print(f'Data 7: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_7=[]
                    Volt15_7=[]
                    Current15_7=[]
                    ActivePower15_7=[]
                    ReactivePower15_7=[]
                    AparentPower15_7=[]
                    FP15_Reactive_7=[]
                    FP15_Inductive_7=[]
                    FDVoltage15_7=[]
                    FDCurrent15_7=[]
                    DAT15Voltage_7=[]
                    DAT15Current_7=[]
               elif(Access_7==1):
                    #print("paso elif 2")
                    Volt15_7.append(Vrms)
                    Current15_7.append(Irms)
                    ActivePower15_7.append(ActivePower)
                    ReactivePower15_7.append(ReactivePower)
                    AparentPower15_7.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_7.append(FP)
                    else: 
                          FP15_Inductive_7.append(FP)
                    FDVoltage15_7.append(FDVoltage)
                    FDCurrent15_7.append(FDCurrent)
                    DAT15Voltage_7.append(DATVoltage)
                    DAT15Current_7.append(DATCurrent)          
    else:
        Volt15_7.append(Vrms)
        Current15_7.append(Irms)
        ActivePower15_7.append(ActivePower)
        ReactivePower15_7.append(ReactivePower)
        AparentPower15_7.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_7.append(FP)
        else: 
              FP15_Inductive_7.append(FP)
        FDVoltage15_7.append(FDVoltage)
        FDCurrent15_7.append(FDCurrent)
        DAT15Voltage_7.append(DATVoltage)
        DAT15Current_7.append(DATCurrent)
        Access_7 = 0
        
        if(len(Volt15_7)>2):
            indice=np.argmin(Volt15_7)
            Volt15_7.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_7)
            Current15_7.pop(indice)
            indice=np.argmin(ActivePower15_7)
            ActivePower15_7.pop(indice)
            indice=np.argmin(ReactivePower15_7)
            ReactivePower15_7.pop(indice)
            indice=np.argmin(AparentPower15_7)
            AparentPower15_7.pop(indice)
            if(len(FP15_Reactive_7)>=2):
                indice=np.argmax(FP15_Reactive_7)
                FP15_Reactive_7.pop(indice)
            if(len(FP15_Inductive_7)>=2):
                indice=np.argmin(FP15_Inductive_7)
                FP15_Inductive_7.pop(indice)
            indice=np.argmin(FDVoltage15_7)
            FDVoltage15_7.pop(indice)
            indice=np.argmin(FDCurrent15_7)
            FDCurrent15_7.pop(indice)
            indice=np.argmin(DAT15Voltage_7)
            DAT15Voltage_7.pop(indice)
            indice=np.argmin(DAT15Current_7)
            DAT15Current_7.pop(indice)

Access_8 = 0
MaxVoltage15_8=0.0
MeanVoltage15_8=0.0
MinVoltage15_8=0.0
MaxCurrent15_8=0.0
MeanCurrent15_8=0.0
MinCurrent15_8=0.0
MaxActivePower_8=0.0
MeanActivePower_8=0.0
MinActivePower_8=0.0
MaxReactivePower_8=0.0
MeanReactivePower_8=0.0
MinReactivePower_8=0.0
MaxAparentPower_8=0.0
MeanAparentPower_8=0.0
MinAparentPower_8=0.0
MaxFPInductive_8=-0.99
MeanFPInductive_8=-0.99
MinFPInductive_8=-0.99
MaxFPReactive_8=0.99
MeanFPReactive_8=0.99
MinFPReactive_8=0.99
MaxFD_8=0.0
MeanFD_8=0.0
MinFD_8=0.0
MaxDAT_8=0.0
MeanDAT_8=0.0
MinDAT_8=0.0
Volt15_8=[]
data15_8=[]
Current15_8=[]
ActivePower15_8=[]
ReactivePower15_8=[]
AparentPower15_8=[]
FP15_Reactive_8=[]
FP15_Inductive_8=[]
FDVoltage15_8=[]
FDCurrent15_8=[]
DAT15Voltage_8=[]
DAT15Current_8=[]
def Maximo15min_8(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_8
    global Volt15_8
    global data15_8
    global Current15_8
    global ActivePower15_8
    global ReactivePower15_8
    global AparentPower15_8
    global FP15_Reactive_8
    global FP15_Inductive_8
    global FDVoltage15_8
    global FDCurrent15_8
    global DAT15Voltage_8
    global DAT15Current_8
    global Access_8
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_8 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_8 = 1
                    MaxVoltage15_8=max(Volt15_8)
                    MeanVoltage15_8=np.median(Volt15_8)
                    MinVoltage15_8=min(Volt15_8)
                    MaxCurrent15_8=max(Current15_8)
                    MeanCurrent15_8=np.median(Current15_8)
                    MinCurrent15_8=min(Current15_8)
                    MaxActivePower_8=max(ActivePower15_8)
                    MeanActivePower_8=np.median(ActivePower15_8)
                    MinActivePower_8=min(ActivePower15_8)
                    MaxReactivePower_8=max(ReactivePower15_8)
                    MeanReactivePower_8=np.median(ReactivePower15_8)
                    MinReactivePower_8=min(ReactivePower15_8)
                    MaxAparentPower_8=max(AparentPower15_8)
                    MeanAparentPower_8=np.median(AparentPower15_8)
                    MinAparentPower_8=min(AparentPower15_8)
                    if(len(FP15_Inductive_8)>0):
                           MaxFPInductive_8=max(FP15_Inductive_8)
                           MeanFPInductive_8=np.median(FP15_Inductive_8)
                           MinFPInductive_8=min(FP15_Inductive_8)
                    else:
                           MaxFPInductive_8=-0.59
                           MeanFPInductive_8=-0.99
                           MinFPInductive_8=-0.99
                    if(len(FP15_Reactive_8)>0):
                           MaxFPReactive_8=max(FP15_Reactive_8)
                           MeanFPReactive_8=np.median(FP15_Reactive_8)
                           MinFPReactive_8=min(FP15_Reactive_8)
                    else:
                           MaxFPReactive_8=0.99
                           MeanFPReactive_8=0.99
                           MinFPReactive_8=0.99
                    MaxFDVoltage_8=max(FDVoltage15_8)
                    MeanFDVoltage_8=np.median(FDVoltage15_8)
                    MinFDVoltage_8=min(FDVoltage15_8)
                    MaxFDCurrent_8=max(FDCurrent15_8)
                    MeanFDCurrent_8=np.median(FDCurrent15_8)
                    MinFDCurrent_8=min(FDCurrent15_8)
                    MaxDATVoltage_8=max(DAT15Volt8ge_8)
                    MeanDATVoltage_8=np.median(DAT15Voltage_8)
                    MinDATVoltage_8=min(DAT15Voltage_8)
                    MaxDATCurrent_8=max(DAT15Current_8)
                    MeanDATCurrent_8=np.median(DAT15Current_8)
                    MinDATCurrent_8=min(DAT15Current_8)
                    data15_8.insert(1,MaxVoltage15_8)
                    data15_8.insert(2,MeanVoltage15_8)
                    data15_8.insert(3,MinVoltage15_8)
                    data15_8.insert(4,MaxCurrent15_8)
                    data15_8.insert(5,MeanCurrent15_8)
                    data15_8.insert(6,MinCurrent15_8)
                    data15_8.insert(7,MaxActivePower_8)
                    data15_8.insert(8,MeanActivePower_8)
                    data15_8.insert(9,MinActivePower_8)
                    data15_8.insert(10,MaxReactivePower_8)
                    data15_8.insert(11,MeanReactivePower_8)
                    data15_8.insert(12,MinReactivePower_8)
                    data15_8.insert(13,MaxAparentPower_8)
                    data15_8.insert(14,MeanAparentPower_8)
                    data15_8.insert(15,MinAparentPower_8)
                    data15_8.insert(16,MaxFPInductive_8)
                    data15_8.insert(17,MeanFPInductive_8)
                    data15_8.insert(18,MinFPInductive_8)
                    data15_8.insert(19,MaxFPReactive_8)
                    data15_8.insert(20,MeanFPReactive_8)
                    data15_5.insert(21,MinFPReactive_8)
                    data15_8.insert(22,MaxFDVoltage_8)
                    data15_8.insert(23,MeanFDVoltage_8)
                    data15_8.insert(24,MinFDVoltage_8)
                    data15_8.insert(25,MaxFDCurrent_8)
                    data15_8.insert(26,MeanFDCurrent_8)
                    data15_8.insert(27,MinFDCurrent_8)
                    data15_8.insert(28,MaxDATVoltage_8)
                    data15_8.insert(29,MeanDATVoltage_8)
                    data15_8.insert(30,MinDATVoltage_8)
                    data15_8.insert(31,MaxDATCurrent_8)
                    data15_8.insert(32,MeanDATCurrent_8)
                    data15_8.insert(33,MinDATCurrent_8)
                    data15_8.insert(34,Energy)
                    data15_8.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet9 = workbook["Max Var 8"]
                    sheet9.append(list(data15_8))
                    print(f'Data 8: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_8=[]
                    Volt15_8=[]
                    Current15_8=[]
                    ActivePower15_8=[]
                    ReactivePower15_8=[]
                    AparentPower15_8=[]
                    FP15_Reactive_8=[]
                    FP15_Inductive_8=[]
                    FDVoltage15_8=[]
                    FDCurrent15_8=[]
                    DAT15Voltage_8=[]
                    DAT15Current_8=[]
               elif(Access_8==1):
                    #print("paso elif 2")
                    Volt15_8.append(Vrms)
                    Current15_8.append(Irms)
                    ActivePower15_8.append(ActivePower)
                    ReactivePower15_8.append(ReactivePower)
                    AparentPower15_8.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_8.append(FP)
                    else: 
                          FP15_Inductive_8.append(FP)
                    FDVoltage15_8.append(FDVoltage)
                    FDCurrent15_8.append(FDCurrent)
                    DAT15Voltage_8.append(DATVoltage)
                    DAT15Current_8.append(DATCurrent)            
    else:
        Volt15_8.append(Vrms)
        Current15_8.append(Irms)
        ActivePower15_8.append(ActivePower)
        ReactivePower15_8.append(ReactivePower)
        AparentPower15_8.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_8.append(FP)
        else: 
              FP15_Inductive_8.append(FP)
        FDVoltage15_8.append(FDVoltage)
        FDCurrent15_8.append(FDCurrent)
        DAT15Voltage_8.append(DATVoltage)
        DAT15Current_8.append(DATCurrent)
        Access_8 = 0       
        if(len(Volt15_8)>2):
            indice=np.argmin(Volt15_8)
            Volt15_8.pop(indice)
            indice=np.argmin(Current15_8)
            Current15_8.pop(indice)
            indice=np.argmin(ActivePower15_8)
            ActivePower15_8.pop(indice)
            indice=np.argmin(ReactivePower15_8)
            ReactivePower15_8.pop(indice)
            indice=np.argmin(AparentPower15_8)
            AparentPower15_8.pop(indice)
            if(len(FP15_Reactive_8)>=2):
                indice=np.argmax(FP15_Reactive_8)
                FP15_Reactive_8.pop(indice)
            if(len(FP15_Inductive_8)>=2):
                indice=np.argmin(FP15_Inductive_8)
                FP15_Inductive_8.pop(indice)
            indice=np.argmin(FDVoltage15_8)
            FDVoltage15_8.pop(indice)
            indice=np.argmin(FDCurrent15_8)
            FDCurrent15_8.pop(indice)
            indice=np.argmin(DAT15Voltage_8)
            DAT15Voltage_8.pop(indice)
            indice=np.argmin(DAT15Current_8)
            DAT15Current_8.pop(indice)

Access_6 = 0
MaxVoltage15_6=0.0
MeanVoltage15_6=0.0
MinVoltage15_6=0.0
MaxCurrent15_6=0.0
MeanCurrent15_6=0.0
MinCurrent15_6=0.0
MaxActivePower_6=0.0
MeanActivePower_6=0.0
MinActivePower_6=0.0
MaxReactivePower_6=0.0
MeanReactivePower_6=0.0
MinReactivePower_6=0.0
MaxAparentPower_6=0.0
MeanAparentPower_6=0.0
MinAparentPower_6=0.0
MaxFPInductive_6=-0.99
MeanFPInductive_6=-0.99
MinFPInductive_6=-0.99
MaxFPReactive_6=0.99
MeanFPReactive_6=0.99
MinFPReactive_6=0.99
MaxFD_6=0.0
MeanFD_6=0.0
MinFD_6=0.0
MaxDAT_6=0.0
MeanDAT_6=0.0
MinDAT_6=0.0
Volt15_6=[]
data15_6=[]
Current15_6=[]
ActivePower15_6=[]
ReactivePower15_6=[]
AparentPower15_6=[]
FP15_Reactive_6=[]
FP15_Inductive_6=[]
FDVoltage15_6=[]
FDCurrent15_6=[]
DAT15Voltage_6=[]
DAT15Current_6=[]
def Maximo15min_6(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_6
    global Volt15_6
    global data15_6
    global Current15_6
    global ActivePower15_6
    global ReactivePower15_6
    global AparentPower15_6
    global FP15_Reactive_6
    global FP15_Inductive_6
    global FDVoltage15_6
    global FDCurrent15_6
    global DAT15Voltage_6
    global DAT15Current_6
    global Access_6
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_6 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_6 = 1
                    MaxVoltage15_6=max(Volt15_6)
                    MeanVoltage15_6=np.median(Volt15_6)
                    MinVoltage15_6=min(Volt15_6)
                    MaxCurrent15_6=max(Current15_6)
                    MeanCurrent15_6=np.median(Current15_6)
                    MinCurrent15_6=min(Current15_6)
                    MaxActivePower_6=max(ActivePower15_6)
                    MeanActivePower_6=np.median(ActivePower15_6)
                    MinActivePower_6=min(ActivePower15_6)
                    MaxReactivePower_6=max(ReactivePower15_6)
                    MeanReactivePower_6=np.median(ReactivePower15_6)
                    MinReactivePower_6=min(ReactivePower15_6)
                    MaxAparentPower_6=max(AparentPower15_6)
                    MeanAparentPower_6=np.median(AparentPower15_6)
                    MinAparentPower_6=min(AparentPower15_6)
                    if(len(FP15_Inductive_6)>0):
                           MaxFPInductive_6=max(FP15_Inductive_6)
                           MeanFPInductive_6=np.median(FP15_Inductive_6)
                           MinFPInductive_6=min(FP15_Inductive_6)
                    else:
                           MaxFPInductive_6=-0.59
                           MeanFPInductive_6=-0.99
                           MinFPInductive_6=-0.99
                    if(len(FP15_Reactive_6)>0):
                           MaxFPReactive_6=max(FP15_Reactive_6)
                           MeanFPReactive_6=np.median(FP15_Reactive_6)
                           MinFPReactive_6=min(FP15_Reactive_6)
                    else:
                           MaxFPReactive_6=0.99
                           MeanFPReactive_6=0.99
                           MinFPReactive_6=0.99
                    MaxFDVoltage_6=max(FDVoltage15_6)
                    MeanFDVoltage_6=np.median(FDVoltage15_6)
                    MinFDVoltage_6=min(FDVoltage15_6)
                    MaxFDCurrent_6=max(FDCurrent15_6)
                    MeanFDCurrent_6=np.median(FDCurrent15_6)
                    MinFDCurrent_6=min(FDCurrent15_6)
                    MaxDATVoltage_6=max(DAT15Voltage_6)
                    MeanDATVoltage_6=np.median(DAT15Voltage_6)
                    MinDATVoltage_6=min(DAT15Voltage_6)
                    MaxDATCurrent_6=max(DAT15Current_6)
                    MeanDATCurrent_6=np.median(DAT15Current_6)
                    MinDATCurrent_6=min(DAT15Current_6)
                    data15_6.insert(1,MaxVoltage15_6)
                    data15_6.insert(2,MeanVoltage15_6)
                    data15_6.insert(3,MinVoltage15_6)
                    data15_6.insert(4,MaxCurrent15_6)
                    data15_6.insert(5,MeanCurrent15_6)
                    data15_6.insert(6,MinCurrent15_6)
                    data15_6.insert(7,MaxActivePower_6)
                    data15_6.insert(8,MeanActivePower_6)
                    data15_6.insert(9,MinActivePower_6)
                    data15_6.insert(10,MaxReactivePower_6)
                    data15_6.insert(11,MeanReactivePower_6)
                    data15_6.insert(12,MinReactivePower_6)
                    data15_6.insert(13,MaxAparentPower_6)
                    data15_6.insert(14,MeanAparentPower_6)
                    data15_6.insert(15,MinAparentPower_6)
                    data15_6.insert(16,MaxFPInductive_6)
                    data15_6.insert(17,MeanFPInductive_6)
                    data15_6.insert(18,MinFPInductive_6)
                    data15_6.insert(19,MaxFPReactive_6)
                    data15_6.insert(20,MeanFPReactive_6)
                    data15_6.insert(21,MinFPReactive_6)
                    data15_6.insert(22,MaxFDVoltage_6)
                    data15_6.insert(23,MeanFDVoltage_6)
                    data15_6.insert(24,MinFDVoltage_6)
                    data15_6.insert(25,MaxFDCurrent_6)
                    data15_6.insert(26,MeanFDCurrent_6)
                    data15_6.insert(27,MinFDCurrent_6)
                    data15_6.insert(28,MaxDATVoltage_6)
                    data15_6.insert(29,MeanDATVoltage_6)
                    data15_6.insert(30,MinDATVoltage_6)
                    data15_6.insert(31,MaxDATCurrent_6)
                    data15_6.insert(32,MeanDATCurrent_6)
                    data15_6.insert(33,MinDATCurrent_6)
                    data15_6.insert(34,Energy)
                    data15_6.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet7 = workbook["Max Var 6"]
                    sheet7.append(list(data15_6))
                    print(f'Data 6: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_6=[]
                    Volt15_6=[]
                    Current15_6=[]
                    ActivePower15_6=[]
                    ReactivePower15_6=[]
                    AparentPower15_6=[]
                    FP15_Reactive_6=[]
                    FP15_Inductive_6=[]
                    FDVoltage15_6=[]
                    FDCurrent15_6=[]
                    DAT15Voltage_6=[]
                    DAT15Current_6=[]
               elif(Access_6==1):
                    #print("paso elif 2")
                    Volt15_6.append(Vrms)
                    Current15_6.append(Irms)
                    ActivePower15_6.append(ActivePower)
                    ReactivePower15_6.append(ReactivePower)
                    AparentPower15_6.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_6.append(FP)
                    else: 
                          FP15_Inductive_6.append(FP)
                    FDVoltage15_6.append(FDVoltage)
                    FDCurrent15_6.append(FDCurrent)
                    DAT15Voltage_6.append(DATVoltage)
                    DAT15Current_6.append(DATCurrent)
              
    else:
        Volt15_6.append(Vrms)
        Current15_6.append(Irms)
        ActivePower15_6.append(ActivePower)
        ReactivePower15_6.append(ReactivePower)
        AparentPower15_6.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_6.append(FP)
        else: 
              FP15_Inductive_6.append(FP)
        FDVoltage15_6.append(FDVoltage)
        FDCurrent15_6.append(FDCurrent)
        DAT15Voltage_6.append(DATVoltage)
        DAT15Current_6.append(DATCurrent)
        Access_6 = 0
        
        if(len(Volt15_6)>2):
            indice=np.argmin(Volt15_6)
            Volt15_6.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_6)
            Current15_6.pop(indice)
            indice=np.argmin(ActivePower15_6)
            ActivePower15_6.pop(indice)
            indice=np.argmin(ReactivePower15_6)
            ReactivePower15_6.pop(indice)
            indice=np.argmin(AparentPower15_6)
            AparentPower15_6.pop(indice)
            if(len(FP15_Reactive_6)>=2):
                indice=np.argmax(FP15_Reactive_6)
                FP15_Reactive_6.pop(indice)
            if(len(FP15_Inductive_6)>=2):
                indice=np.argmin(FP15_Inductive_6)
                FP15_Inductive_6.pop(indice)
            indice=np.argmin(FDVoltage15_6)
            FDVoltage15_6.pop(indice)
            indice=np.argmin(FDCurrent15_6)
            FDCurrent15_6.pop(indice)
            indice=np.argmin(DAT15Voltage_6)
            DAT15Voltage_6.pop(indice)
            indice=np.argmin(DAT15Current_6)
            DAT15Current_6.pop(indice)

Access_9 = 0
MaxVoltage15_9=0.0
MeanVoltage15_9=0.0
MinVoltage15_9=0.0
MaxCurrent15_9=0.0
MeanCurrent15_9=0.0
MinCurrent15_9=0.0
MaxActivePower_9=0.0
MeanActivePower_9=0.0
MinActivePower_9=0.0
MaxReactivePower_9=0.0
MeanReactivePower_9=0.0
MinReactivePower_9=0.0
MaxAparentPower_9=0.0
MeanAparentPower_9=0.0
MinAparentPower_9=0.0
MaxFPInductive_9=-0.99
MeanFPInductive_9=-0.99
MinFPInductive_9=-0.99
MaxFPReactive_9=0.99
MeanFPReactive_9=0.99
MinFPReactive_9=0.99
MaxFD_9=0.0
MeanFD_9=0.0
MinFD_9=0.0
MaxDAT_9=0.0
MeanDAT_9=0.0
MinDAT_9=0.0
Volt15_9=[]
data15_9=[]
Current15_9=[]
ActivePower15_9=[]
ReactivePower15_9=[]
AparentPower15_9=[]
FP15_Reactive_9=[]
FP15_Inductive_9=[]
FDVoltage15_9=[]
FDCurrent15_9=[]
DAT15Voltage_9=[]
DAT15Current_9=[]
def Maximo15min_9(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_9
    global Volt15_9
    global data15_9
    global Current15_9
    global ActivePower15_9
    global ReactivePower15_9
    global AparentPower15_9
    global FP15_Reactive_9
    global FP15_Inductive_9
    global FDVoltage15_9
    global FDCurrent15_9
    global DAT15Voltage_9
    global DAT15Current_9
    global Access_9
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==15 or basea.minute==16 or basea.minute==30 or basea.minute==31 or basea.minute==45 or basea.minute==46):
               if(Access_9 == 0):
                    #graphVoltage(NoVoltageoffset2,ListaIrmsPeak2,samplings2,2)
                    Access_9 = 1
                    MaxVoltage15_9=max(Volt15_9)
                    MeanVoltage15_9=np.median(Volt15_9)
                    MinVoltage15_9=min(Volt15_9)
                    MaxCurrent15_9=max(Current15_9)
                    MeanCurrent15_4=np.median(Current15_9)
                    MinCurrent15_9=min(Current15_9)
                    MaxActivePower_9=max(ActivePower15_9)
                    MeanActivePower_9=np.median(ActivePower15_9)
                    MinActivePower_9=min(ActivePower15_9)
                    MaxReactivePower_9=max(ReactivePower15_9)
                    MeanReactivePower_9=np.median(ReactivePower15_9)
                    MinReactivePower_9=min(ReactivePower15_9)
                    MaxAparentPower_9=max(AparentPower15_9)
                    MeanAparentPower_9=np.median(AparentPower15_9)
                    MinAparentPower_9=min(AparentPower15_9)
                    if(len(FP15_Inductive_9)>0):
                           MaxFPInductive_9=max(FP15_Inductive_9)
                           MeanFPInductive_9=np.median(FP15_Inductive_9)
                           MinFPInductive_9=min(FP15_Inductive_9)
                    else:
                           MaxFPInductive_9=-0.99
                           MeanFPInductive_9=-0.99
                           MinFPInductive_9=-0.99
                    if(len(FP15_Reactive_9)>0):
                           MaxFPReactive_9=max(FP15_Reactive_9)
                           MeanFPReactive_9=np.median(FP15_Reactive_9)
                           MinFPReactive_9=min(FP15_Reactive_9)
                    else:
                           MaxFPReactive_9=0.99
                           MeanFPReactive_9=0.99
                           MinFPReactive_9=0.99
                    MaxFDVoltage_9=max(FDVoltage15_9)
                    MeanFDVoltage_9=np.median(FDVoltage15_9)
                    MinFDVoltage_9=min(FDVoltage15_9)
                    MaxFDCurrent_9=max(FDCurrent15_9)
                    MeanFDCurrent_9=np.median(FDCurrent15_9)
                    MinFDCurrent_9=min(FDCurrent15_9)
                    MaxDATVoltage_9=max(DAT15Voltage_9)
                    MeanDATVoltage_9=np.median(DAT15Voltage_9)
                    MinDATVoltage_9=min(DAT15Voltage_9)
                    MaxDATCurrent_9=max(DAT15Current_9)
                    MeanDATCurrent_9=np.median(DAT15Current_9)
                    MinDATCurrent_9=min(DAT15Current_9)
                    data15_9.insert(1,MaxVoltage15_9)
                    data15_9.insert(2,MeanVoltage15_9)
                    data15_9.insert(3,MinVoltage15_9)
                    data15_9.insert(4,MaxCurrent15_9)
                    data15_9.insert(5,MeanCurrent15_9)
                    data15_9.insert(6,MinCurrent15_9)
                    data15_9.insert(7,MaxActivePower_9)
                    data15_9.insert(8,MeanActivePower_9)
                    data15_9.insert(9,MinActivePower_9)
                    data15_9.insert(10,MaxReactivePower_9)
                    data15_9.insert(11,MeanReactivePower_9)
                    data15_9.insert(12,MinReactivePower_9)
                    data15_9.insert(13,MaxAparentPower_9)
                    data15_9.insert(14,MeanAparentPower_9)
                    data15_9.insert(15,MinAparentPower_9)
                    data15_9.insert(16,MaxFPInductive_9)
                    data15_9.insert(17,MeanFPInductive_9)
                    data15_9.insert(18,MinFPInductive_9)
                    data15_9.insert(19,MaxFPReactive_9)
                    data15_9.insert(20,MeanFPReactive_9)
                    data15_9.insert(21,MinFPReactive_9)
                    data15_9.insert(22,MaxFDVoltage_9)
                    data15_9.insert(23,MeanFDVoltage_9)
                    data15_9.insert(24,MinFDVoltage_9)
                    data15_9.insert(25,MaxFDCurrent_9)
                    data15_9.insert(26,MeanFDCurrent_9)
                    data15_9.insert(27,MinFDCurrent_9)
                    data15_9.insert(28,MaxDATVoltage_9)
                    data15_9.insert(29,MeanDATVoltage_9)
                    data15_9.insert(30,MinDATVoltage_9)
                    data15_9.insert(31,MaxDATCurrent_9)
                    data15_9.insert(32,MeanDATCurrent_9)
                    data15_9.insert(33,MinDATCurrent_9)
                    data15_9.insert(34,Energy)
                    data15_9.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet10 = workbook["Max Var 9"]
                    sheet10.append(list(data15_9))
                    print(f'Data 9: Guardando Promedios')
                    #print("Datos Insertados Correctamente!")
                    workbook.save(filename = dest_filename)
                    data15_9=[]
                    Volt15_9=[]
                    Current15_9=[]
                    ActivePower15_9=[]
                    ReactivePower15_9=[]
                    AparentPower15_9=[]
                    FP15_Reactive_9=[]
                    FP15_Inductive_9=[]
                    FDVoltage15_9=[]
                    FDCurrent15_9=[]
                    DAT15Voltage_9=[]
                    DAT15Current_9=[]
               elif(Access_9==1):
                    #print("paso elif 2")
                    Volt15_9.append(Vrms)
                    Current15_9.append(Irms)
                    ActivePower15_9.append(ActivePower)
                    ReactivePower15_9.append(ReactivePower)
                    AparentPower15_9.append(AparentPower)
                    if(FP>0.0):
                          FP15_Reactive_9.append(FP)
                    else: 
                          FP15_Inductive_9.append(FP)
                    FDVoltage15_9.append(FDVoltage)
                    FDCurrent15_9.append(FDCurrent)
                    DAT15Voltage_9.append(DATVoltage)
                    DAT15Current_9.append(DATCurrent)
              
    else:
        Volt15_9.append(Vrms)
        Current15_9.append(Irms)
        ActivePower15_9.append(ActivePower)
        ReactivePower15_9.append(ReactivePower)
        AparentPower15_9.append(AparentPower)
        if(FP>0.0):
              FP15_Reactive_9.append(FP)
        else: 
              FP15_Inductive_9.append(FP)
        FDVoltage15_9.append(FDVoltage)
        FDCurrent15_9.append(FDCurrent)
        DAT15Voltage_9.append(DATVoltage)
        DAT15Current_9.append(DATCurrent)
        Access_9 = 0
        
        if(len(Volt15_9)>2):
            indice=np.argmin(Volt15_9)
            Volt15_9.pop(indice)
            ##print(f'Volt152 Despúes: {Volt152}')
            indice=np.argmin(Current15_9)
            Current15_9.pop(indice)
            indice=np.argmin(ActivePower15_9)
            ActivePower15_9.pop(indice)
            indice=np.argmin(ReactivePower15_9)
            ReactivePower15_9.pop(indice)
            indice=np.argmin(AparentPower15_9)
            AparentPower15_9.pop(indice)
            if(len(FP15_Reactive_9)>=2):
                indice=np.argmax(FP15_Reactive_9)
                FP15_Reactive_4.pop(indice)
            if(len(FP15_Inductive_9)>=2):
                indice=np.argmin(FP15_Inductive_9)
                FP15_Inductive_9.pop(indice)
            indice=np.argmin(FDVoltage15_9)
            FDVoltage15_9pop(indice)
            indice=np.argmin(FDCurrent15_9)
            FDCurrent15_9.pop(indice)
            indice=np.argmin(DAT15Voltage_9)
            DAT15Voltage_9.pop(indice)
            indice=np.argmin(DAT15Current_9)
            DAT15Current_9.pop(indice)

def excelcreate():
    global dest_filename
    global sheet1
    global sheet2
    global sheet3
    global sheet4
    global sheet5
    global sheet6
    global sheet7
    global sheet8
    global sheet9
    global sheet10
    global sheet11
    global sheet12
    global sheet13
    global sheet14
    global sheet15
    global sheet16
    global sheet17
    global sheet18
    global sheet19
    from openpyxl import Workbook
    exceltime=date.today()
    book = Workbook()
    dest_filename = f'{exceltime}.xlsx'
    #sheet1 = book.active
    sheet1  = book.create_sheet("Var 0")
    sheet2 = book.create_sheet("Max Var 1")
    sheet3 = book.create_sheet("Max Var 2")
    sheet4 = book.create_sheet("Max Var 3")
    sheet5 = book.create_sheet("Max Var 4")
    sheet6 = book.create_sheet("Max Var 5")
    sheet7 = book.create_sheet("Max Var 6")
    sheet8 = book.create_sheet("Max Var 7")
    sheet9 = book.create_sheet("Max Var 8")
    sheet10 = book.create_sheet("Max Var 9")
    sheet11 = book.create_sheet("Var 1")
    sheet12 = book.create_sheet("Var 2")
    sheet13 = book.create_sheet("Var 3")
    sheet14 = book.create_sheet("Var 4")
    sheet15 = book.create_sheet("Var 5")
    sheet16 = book.create_sheet("Var 6")
    sheet17 = book.create_sheet("Var 7")
    sheet18 = book.create_sheet("Var 8")
    sheet19 = book.create_sheet("Var 9")
    headings0 = ['Fecha y Hora'] + list(['T° Raspberry','Uso CPU %','RAM2'])
    headings=['Fecha y Hora'] + list(['Max Voltage','Mean Voltage','Min Voltage', 'Max Current','Mean Current','Min Current','Max Active Power','Mean Active Power','Mean Active Power','Max Reactive Power','Mean Reactive Power','Min Reactive Power','Max Aparent Power','Mean Aparent Power','Min Aparent Power','Max FPReact ','Mean FPReact','Min FPReact','Max FPInduct','Mean FPInduct','Min FPInduct','Max FDVoltage','Mean FDVoltage','Min FDVoltage','Max FDCurrent','Mean FDCurrent','Min FDCurrent','Max DATVoltage','Mean DATVoltage','Min DATVoltage','Max DATCurrent','Mean DATCurrent','Min DATCurrent','Energy'])
    headings2=['Fecha y Hora'] + list(['Voltage', 'Current','Active Power','Reactive Power','Aparent Power','FP','FDVoltage','FDCurrent','DATVoltage','DATCurrent','cos(phi)','Energy','Hour Energy'])
    headings3=['Fecha y Hora'] + list(['Voltage', 'Current','Power','Energy','Hour Energy'])
    headings4=['Fecha y Hora'] + list(['Max Voltage', 'Mean Voltage', 'Min Voltage', 'Max Current','Mean Current', 'Min Current','Max Power','Power Mean', 'Power','Total Energy','Energy acumulada en 15'])
    ceros=list([0,0,0,0,0,0,0,0,0,0,0,0])
    sheet1.append(headings0)
    sheet2.append(headings)
    sheet3.append(headings)
    sheet4.append(headings)
    sheet5.append(headings)
    sheet6.append(headings)
    sheet7.append(headings)
    sheet8.append(headings)
    sheet9.append(headings)
    sheet10.append(headings2)
    sheet11.append(headings2)
    sheet12.append(headings2)
    sheet13.append(headings2)
    sheet14.append(headings2)
    sheet15.append(headings2)
    sheet16.append(headings2)
    sheet17.append(headings2)
    sheet18.append(headings2)
    sheet19.append(headings2)
    sheet1.append(list([0,0,0]))
    sheet2.append(ceros)
    sheet3.append(ceros)
    sheet4.append(ceros)
    sheet5.append(ceros)
    sheet6.append(ceros)
    sheet7.append(ceros)
    sheet8.append(ceros)
    sheet9.append(ceros)
    sheet10.append(ceros)
    sheet11.append(ceros)
    sheet12.append(ceros)
    sheet13.append(ceros)
    sheet14.append(ceros)
    sheet15.append(ceros)
    sheet16.append(ceros)
    sheet17.append(ceros)
    sheet18.append(ceros)
    sheet19.append(ceros)

    book.save(filename = dest_filename)



def AbrirExcel():
    global dest_filename
    global Energy_1
    global Energy_2
    global Energy_3
    global Energy_4
    global Energy_5
    global Energy_6
    global Energy_7
    global Energy_8
    global Energy_9
    
    dia=date.today()
    if(os.path.exists(f'{dia}.xlsx')):
            dest_filename = f'{dia}.xlsx'
            print("Existe")
            workbook=openpyxl.load_workbook(filename = dest_filename)
            sheet11 = workbook["Var 1"]
            sheet12 = workbook["Var 2"]
            sheet13 = workbook["Var 3"]
            sheet14 = workbook["Var 4"]
            sheet15 = workbook["Var 5"]
            sheet16 = workbook["Var 6"]
            sheet17 = workbook["Var 7"]
            sheet18 = workbook["Var 8"]
            sheet19 = workbook["Var 9"]
            LargeSheet11=len(sheet11["FP"])
            LargeSheet12=len(sheet12["FP"])
            LargeSheet13=len(sheet13["FP"])
            LargeSheet14=len(sheet14["FP"])
            LargeSheet15=len(sheet15["FP"])
            LargeSheet16=len(sheet16["FP"])
            LargeSheet17=len(sheet17["FP"])
            LargeSheet18=len(sheet18["FP"])
            LargeSheet19=len(sheet19["FP"])
            print("Largo Excel Var 1: ",LargeSheet11)
            Energy_1 = float(sheet11[f'k{LargeSheet11}'].value)
            Energy_2 = float(sheet12[f'k{LargeSheet12}'].value)
            Energy_3 = float(sheet13[f'k{LargeSheet13}'].value)
            Energy_4 = float(sheet14[f'k{LargeSheet14}'].value)
            Energy_5 = float(sheet15[f'k{LargeSheet15}'].value)
            Energy_6 = float(sheet16[f'k{LargeSheet16}'].value)
            Energy_7 = float(sheet17[f'k{LargeSheet17}'].value)
            Energy_8 = float(sheet18[f'k{LargeSheet18}'].value)
            Energy_9 = float(sheet19[f'k{LargeSheet19}'].value)
            #energyBaterias = float(sheet8[f'k{largoexcelCGE-2}'].value)
            #energyPanelesDC = float(sheet10[f'k{largoexcelCGE-2}'].value)
            print(f'Valor Energia  {Energy_1} ')
    else:
            excelcreate()
            print("No Existe")

AbrirExcel()


def VariablesBasicas(Temp_Raspberry,cpu_uso,RAM,tempESP32):                   
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet1 = workbook["Var 0"]
       Data=[datetime.datetime.now(),round(Temp_Raspberry,1), round(cpu_uso,1), round(RAM,1), round(tempESP32,2)]
       sheet1.append(list(Data))
       workbook.save(filename = dest_filename)
       Data=[]

def SaveDataCsv(Vrms,Irms,ActivePower_1,ReactivePower_1,AparentPower_1,FP_1,CosPhi_1,FDVoltage_1,FDCurrent_1,DATVoltage_1,DATCurrent_1,Energy_1,OneHourEnergy_1,i):
       Data=[datetime.datetime.now(),round(Vrms,2), round(Irms,2), round(ActivePower_1,2), round(ReactivePower_1,2), round(AparentPower_1,2), round(FP_1,2), round(CosPhi_1,2), round(FDVoltage_1,2), round(FDCurrent_1,2), round(DATVoltage_1,2), round(DATCurrent_1,2), round(Energy_1,5), round(OneHourEnergy_1,5)]                    
       workbook=openpyxl.load_workbook(filename = dest_filename)
       if(i==1):
             sheet11 = workbook["Var 1"]
             #dataCGEAll.insert(0,datetime.datetime.now())
             sheet11.append(list(Data))
       elif(i==2):
             sheet12 = workbook["Var 2"]
             sheet12.append(list(Data))
       elif(i==3):
             sheet13 = workbook["Var 3"]
             sheet13.append(list(Data))
       elif(i==4):
             sheet14 = workbook["Var 4"]
             sheet14.append(list(Data))
       elif(i==5):
             sheet15 = workbook["Var 5"]
             sheet15.append(list(Data))
       elif(i==6):
             sheet16 = workbook["Var 6"]
             sheet16.append(list(Data))
       elif(i==7):
             sheet17 = workbook["Var 7"]
             sheet17.append(list(Data))
       elif(i==8):
             sheet18 = workbook["Var 8"]
             sheet18.append(list(Data))
       elif(i==9):
             sheet19 = workbook["Var 9"]
             sheet19.append(list(Data))
       
       
       workbook.save(filename = dest_filename)
       Data=[]



def ExcelData_15():                       
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet2 = workbook["CGE Maximos 15 Min"]
       sheet2.append(list(dataCGE))
       #print(f'Data CGE: {dataCGE}')
       #print("Datos Insertados Correctamente!")
       workbook.save(filename = dest_filename)
       dataCGE=[]
      
def ExcelData15_1():
       global data15_1All
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet6 = workbook["2"]
       data15_1All.insert(0,datetime.datetime.now())
       sheet6.append(list(data15_1All))
       #print(f'Data 2: {data15_1}')
       #print("Datos Insertados Correctamente!")
       workbook.save(filename = dest_filename)
       data15_1All=[]




def SendEmail():
    #global dest_filename
    Lugar="Santa Cristina"
    username = "empresasserspa@gmail.com"
    password = "empresasserspa"
    destinatario = "ricardovera.93@hotmail.com"
    #destinatario2 = "ricardovera.93@hotmail.com"
    #destinatario2 = "demetrio.vera@serm.cl"
    
    mensaje = MIMEMultipart("Alternative")
    mensaje["Subject"] = "Reportes "+str(Lugar)+" "+str(datetime.date.today())
    mensaje["From"] = username
    mensaje["To"] = destinatario
    
    html = f"""
    <html>
    <body>
         <p> Hola <i>{destinatario}</i> <br>
         Reportes desde {Lugar} </b>
    </body>
    </html>
    """
    
    parte_html = MIMEText(html, "html")
    mensaje.attach(parte_html)
    
    archivo = dest_filename
    
    with open(archivo, "rb") as adjunto:
         contenido_adjunto = MIMEBase("application", "octet-stream")
         contenido_adjunto.set_payload(adjunto.read())
    
    encoders.encode_base64(contenido_adjunto)
    
    contenido_adjunto.add_header(
         "Content-Disposition",
         f"attachment; filename= {archivo}",
    )

    mensaje.attach(contenido_adjunto)
    text = mensaje.as_string()
    
    
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
          server.login(username,password)
          print("Sesión Iniciada Correctamente !")
          #server.sendmail(username, destinatario, mensaje)
          server.sendmail(username, destinatario, text)
          #server.sendmail(username, destinatario2, text)
          print("Mensaje Enviado Correctamente !")


"""
while client.connected_flag: 
    #print("In loop")
    #received()
    publish(client)
    #time.sleep(5)
"""   


fecha=str(datetime.datetime.now())

f = open('mi_fichero2.txt', 'w')
try:
    f.write(fecha)
finally:
    f.close()


Vrms=0.0
Irms=0.0
BufferVoltaje=[]
BufferCurrent=[]
global Access_1email
Access_1email=0
global Access_1excel
Access_1xcel=0
countbroker=0

def TomaDatos(list_Voltage,list_Current,samplings,i):
    global BufferVoltaje
    global BufferCurrent
    global Vrms
    global Irms
    global MediaBufferVoltaje
    global MediaBufferCurrent
    global NoVoltageOffset
    global NoCurrentoffset
    global ListaIrmsPeak1
                           
    sos = signal.butter(10, 2500, 'low', fs=samplings, output='sos')
    list_VoltageFilterComplete = signal.sosfilt(sos, list_Voltage)
    list_CurrentFilterComplete = signal.sosfilt(sos, list_Current)
    list_FinalVoltage = list_VoltageFilterComplete[104:4200]
    list_FinalCurrent = list_CurrentFilterComplete [103:4200]

    List_MaxVoltage=getMaxValues(list_FinalVoltage, 50)
    List_MinVoltage=getMinValues(list_FinalVoltage, 50)
    MaxVoltage = np.median(List_MaxVoltage)
    MinVoltage = np.median(List_MinVoltage)
    DC_VoltageMedian = (MaxVoltage+MinVoltage)/2
    NoVoltageOffset=(list_FinalVoltage-DC_VoltageMedian)
                           
    Vrms=VoltajeRms(NoVoltageOffset)*0.92
                          
    if (len(BufferVoltaje)>=5):
        MediaBufferVoltaje=np.median(BufferVoltaje)
        Vrms=VoltRms(MediaBufferVoltaje)
        print(f'Vrms {i}: {Vrms}')
        #str_num = {"value":Vrms,"save":1}
        #Vrms1 = json.dumps(str_num)
        VoltageFFT(NoVoltageOffset,samplings,i)
        BufferVoltaje=[]
    else:
        BufferVoltaje.append(Vrms)
                        

    #Valor dc de corriente
    List_MaxCurrent=getMaxValues(list_FinalCurrent, 50)
    List_MinCurrent=getMinValues(list_FinalCurrent, 50)
    MaxCurrent = np.median(List_MaxCurrent)
    Mincurrent = np.median(List_MinCurrent)
    DC_CurrentMedian = (MaxCurrent+Mincurrent)/2
    NoCurrentoffset=list_FinalCurrent-DC_CurrentMedian
    Irms=CorrienteRms(NoCurrentoffset)
                               
    
    if (len(BufferCurrent)>=5):
        MediaBufferCurrent=np.median(BufferCurrent)
        Irms=CurrentRms(MediaBufferCurrent)*0.885
        print(f'Irms {i}: {Irms}')
        #str_num = {"value":Irms,"save":1}
        #Irms1 = json.dumps(str_num)
        CurrentFFT(NoCurrentoffset,samplings,i,Irms)
        potrmsCGE = PotenciaRms(NoCurrentoffset,NoVoltageOffset)
        Potencias(i,Irms,Vrms,potrmsCGE)     
        BufferCurrent=[]
    else:
        BufferCurrent.append(Irms)

def received():
    while True:
                 try:
                     esp32_bytes = esp32.readline()
                     decoded_bytes = str(esp32_bytes[0:len(esp32_bytes)-2].decode("utf-8"))#utf-8
                 except:
                     print("Error en la codificación")
                     continue
                 np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')   
                 if (len(np_array) == 8402):
                       if (np_array[0] == 11 or np_array[0] == 22 or np_array[0] == 33 or np_array[0] == 44 or np_array[0] == 55 or np_array[0] == 66 or np_array[0] == 77 or np_array[0] == 88 or np_array[0] == 99):
                           if (np_array[0] == 11):
                               i = 1
                           elif (np_array[0] == 22):
                               i = 2
                           elif (np_array[0] == 33):
                               i = 3
                           elif (np_array[0] == 44):
                               i = 4
                           elif (np_array[0] == 55):
                               i = 5
                           elif (np_array[0] == 66):
                               i = 6
                           elif (np_array[0] == 77):
                               i = 7
                           elif (np_array[0] == 88):
                               i = 8
                           elif (np_array[0] == 99):
                               i = 9
                           samplings = np_array[-1]
                           list_Voltage = (np_array[0:4200])
                           list_Current = np_array[4201:8400]
                           try:
                                 TomaDatos(list_Voltage,list_Current,samplings,i)
                           except OSError as err:
                                 print("OS error: {0}".format(err))
                                 continue
                           except ValueError:
                                 print("Could not convert data to an integer.")
                                 continue
                 if (len(np_array)>0 and len(np_array)<=2):
                         Temp_Raspberry=cpu_temp()
                         cpu_uso=get_cpuload()
                         tempESP32 = round(np_array[0],0)
                         RAM = psutil.virtual_memory()[2]
                         VariablesBasicas(Temp_Raspberry,cpu_uso,RAM,tempESP32)
                         if (RAM > 93):
                              os.system("sudo reboot")
                         #Temp_Raspberry_JSON = json.dumps(str_num)
                         #Ventilador()
                         #temphum()
                         #distance()
                         #str_num2 = {"value":tempESP320,"save":0}
                         #str_num = {"value":Temp_Raspberry0,"save":0}
                         #tempESP32 = json.dumps(str_num2)
                         

                 excel=datetime.datetime.now()
                 if(excel.hour==0 and excel.minute==3):
                          if(Access_1email==0):
                                 Access_1email=1
                                 print("Entro a SendEmail")
                                 #SendEmail()
                                 time.sleep(5)
                                 #os.remove(dest_filename)
                                 excelcreate()
                 else:
                     Access_1email=0
                 """
                 try:  
                       if(client.connected_flag==True): 
                             publish(client)
                 except:
                     global countbroker
                     print(f'Count Broker: {countbroker}')
                     if(countbroker>=71):
                         reconnectmqtt()
                         countbroker=0
                     else: 
                         countbroker=countbroker+1
                         
                     continue
                 """

        
if __name__ == '__main__':
    received()
    #t = threading.Thread(target=received)
    #t.daemon = True
    #t.start()

