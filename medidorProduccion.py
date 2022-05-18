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

    



def VoltageFFT(list_fftVoltages, samplings,i):
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
    j = str(i)
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
           global sincvoltaje1            
           PhaseVoltage = np.arctan(real[0]/(imag[0]))
           FDVoltage = Magnitud1/SumMagnitudEficaz
           DATVoltage= np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
           sincvoltaje1 = 1
           str_num_FD = {"value":FDVoltage,"save":1}
           str_num_DAT = {"value":DATVoltage,"save":1}
           if (i == 1):
               FDVoltage_1 = FDVoltage
               DATVoltage_1= DATVoltage
               FDVoltage_1 = json.dumps(str_num_FD)  
               DATVoltage_1 = json.dumps(str_num_DAT)
           elif (i == 2):
               FDVoltage_2 = FDVoltage
               DATVoltage_2= DATVoltage
               FDVoltage_2 = json.dumps(str_num_FD)  
               DATVoltage_2 = json.dumps(str_num_DAT)             
           elif (i == 3:
               FDVoltage_3 = FDVoltage
               DATVoltage_3= DATVoltage
               FDVoltage_3 = json.dumps(str_num_FD)  
               DATVoltage_3 = json.dumps(str_num_DAT)             
           elif (i == 4):
               FDVoltage_4 = FDVoltage
               DATVoltage_4= DATVoltage
               FDVoltage_4 = json.dumps(str_num_FD)  
               DATVoltage_4 = json.dumps(str_num_DAT)             
           elif (i == 5):
               FDVoltage_5 = FDVoltage
               DATVoltage_5= DATVoltage
               FDVoltage_5 = json.dumps(str_num_FD)  
               DATVoltage_5 = json.dumps(str_num_DAT)             
           elif (i == 6):
               FDVoltage_6 = FDVoltage
               DATVoltage_6= DATVoltage
               FDVoltage_6 = json.dumps(str_num_FD)  
               DATVoltage_6 = json.dumps(str_num_DAT)             
           elif (i == 7):
               FDVoltage_7 = FDVoltage
               DATVoltage_7= DATVoltage
               FDVoltage_7 = json.dumps(str_num_FD)  
               DATVoltage_7 = json.dumps(str_num_DAT)             
           elif (i == 8):
               FDVoltage_8 = FDVoltage
               DATVoltage_8= DATVoltage
               FDVoltage_8 = json.dumps(str_num_FD)  
               DATVoltage_8 = json.dumps(str_num_DAT)             
           elif (i == 9):
               FDVoltage_9 = FDVoltage
               DATVoltage_9= DATVoltage
               FDVoltage_9 = json.dumps(str_num_FD)  
               DATVoltage_9 = json.dumps(str_num_DAT)             
           


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

def CurrentFFT(list_fftVoltages, samplings, i,Irms):
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
    q = str(i)
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
         global sincvoltaje1
         FDCurrent = Irmsarmonico1prop/Irms
         str_num_FD = {"value":FDCurrent,"save":0}
         JsonFDCurrent = json.dumps(str_num_FD_Current)
         DATCurrent = np.sqrt((SumMagnitudEficaz2**2-Magnitud1**2)/(Magnitud1**2))
         str_num_DAT = {"value":DATCurrent,"save":0}
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
             if (i == 1):
                 CosPhi_1=CosPhi
                 FP_1=FP
                 DATCurrent_1=DATCurrent
                 FDCurrent_1=FDCurrent
                 FDCurrentJson1 = json.dumps(str_num_FD)  
                 DATCurrentJson1 = json.dumps(str_num_DAT)
                 FPCurrentJson1 = json.dumps(str_num_FP) 
             elif (i == 2):
                 CosPhi_2=CosPhi
                 FP_2=FP
                 DATCurrent_2=DATCurrent
                 FDCurrent2=FDCurrent
                 FDCurrentJson2 = json.dumps(str_num_FD)  
                 DATCurrentJson2 = json.dumps(str_num_DAT)  
                 FPCurrentJson2 = json.dumps(str_num_FP)            
             elif (i == 3:
                 CosPhi_3=CosPhi
                 FP_3=FP
                 DATCurrent_3=DATCurrent
                 FDCurrent_3=FDCurrent
                 FDCurrentJson3 = json.dumps(str_num_FD)  
                 DATCurrentJson3 = json.dumps(str_num_DAT)
                 FPCurrentJson3 = json.dumps(str_num_FP)              
             elif (i == 4):
                 CosPhi_4=CosPhi
                 FP_4=FP
                 DATCurrent_4=DATCurrent
                 FDCurrent_4=FDCurrent
                 FDCurrentJson4 = json.dumps(str_num_FD)  
                 DATCurrentJson4 = json.dumps(str_num_DAT)
                 FPCurrentJson4 = json.dumps(str_num_FP)              
             elif (i == 5):
                 CosPhi_5=CosPhi
                 FP_5=FP
                 DATCurrent_5=DATCurrent
                 FDCurrent_5=FDCurrent
                 FDCurrentJson5 = json.dumps(str_num_FD)  
                 DATCurrentJson5 = json.dumps(str_num_DAT)
                 FPCurrentJson5 = json.dumps(str_num_FP)              
             elif (i == 6):
                 CosPhi_6=CosPhi
                 FP_6=FP
                 DATCurrent_6=DATCurrent
                 FDCurrent_6=FDCurrent
                 FDCurrentJson6 = json.dumps(str_num_FD)  
                 DATCurrentJson6 = json.dumps(str_num_DAT)
                 FPCurrentJson6 = json.dumps(str_num_FP)             
             elif (i == 7):
                 CosPhi_7=CosPhi
                 FP_7=FP
                 DATCurrent_7=DATCurrent
                 FDCurrent_7=FDCurrent
                 FDCurrentJson7 = json.dumps(str_num_FD)  
                 DATCurrentJson7 = json.dumps(str_num_DAT)
                 FPCurrentJson7 = json.dumps(str_num_FP)              
             elif (i == 8):
                 CosPhi_8=CosPhi
                 FP_8=FP
                 DATCurrent_8=DATCurrent
                 FDCurrent_8=FDCurrent
                 FDCurrentJson8 = json.dumps(str_num_FD)  
                 DATCurrent8son8 = json.dumps(str_num_DAT)
                 FPCurrentJson8 = json.dumps(str_num_FP)              
             elif (i == 9):
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
Energy1 = 0.0
Energy2 = 0.0
Energy3 = 0.0
Energy4 = 0.0
Energy5 = 0.0
Energy6 = 0.0
Energy7 = 0.0
Energy8 = 0.0
Energy9 = 0.0
OneHourEnergy1 = 0.0
OneHourEnergy2 = 0.0
OneHourEnergy3 = 0.0
OneHourEnergy4 = 0.0
OneHourEnergy5 = 0.0
OneHourEnergy6 = 0.0
OneHourEnergy7 = 0.0
OneHourEnergy8 = 0.0
OneHourEnergy9 = 0.0
AparentPower = 0.0
ActivePower = 0.0
ReactivePower = 0.0


def Potencias(i,Irms,Vrms,potrmsCGE):
    i = str(i)
    global a
    global Energy
    global ActivePower
    global AparentPower
    global ReactivePower
    TimeEnergy = datetime.datetime.now()
    if(TimeEnergy.minute==0):
            OneHourEnergy1=0
            OneHourEnergy2=0
            OneHourEnergy3=0
            OneHourEnergy4=0
            OneHourEnergy5=0
            OneHourEnergy6=0
            OneHourEnergy7=0
            OneHourEnergy8=0
            OneHourEnergy9=0
    if(TimeEnergy.hour==0 and TimeEnergy.minute==0):
            Energy1=0
            Energy2=0
            Energy3=0
            Energy4=0
            Energy5=0
            Energy6=0
            Energy7=0
            Energy8=0
            Energy9=0
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
        SaveDataCsv_1(Vrms,Irms,ActivePower_1,ReactivePower_1,AparentPower_1,FP_1,CosPhi_1,FDVoltage_1,FDCurrent_1,DATVoltage_1,DATCurrent_1,Energy_1,OneHourEnergy_1):
        #Maximo15min_1(Vrms,Irms,ActivePower_1,ReactivePower_1,AparentPower_1,FP_1,FDVoltage_1,FDCurrent_1,DATVoltage_1,DATCurrent_1,Energy_1):
    elif (i == 2):
        Time2b = datetime.datetime.now()
        delta=(((Time2b - Time2a).microseconds)/1000+((Time2b - Time2a).seconds)*1000)/10000000000
        Energy_2 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_2 += np.abs(ActivePower*delta*2.9)
        Time2a = datetime.datetime.now()
        AparentPower_2 = AparentPower
        ActivePower_2 = ActivePower
        ReactivePower_2 = ReactivePower        
    elif (i == 3):
        Time3b = datetime.datetime.now()
        delta=(((Time3b - Time3a).microseconds)/1000+((Time3b - Time3a).seconds)*1000)/10000000000
        Energy_3 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_3 += np.abs(ActivePower*delta*2.9)
        Time3a = datetime.datetime.now()
        AparentPower_3 = AparentPower
        ActivePower_3 = ActivePower
        ReactivePower_3 = ReactivePower            
    elif (i == 4):
        Time4b = datetime.datetime.now()
        delta=(((Time4b - Time4a).microseconds)/1000+((Time4b - Time4a).seconds)*1000)/10000000000
        Energy_4 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_4 += np.abs(ActivePower*delta*2.9)
        Time4a = datetime.datetime.now()
        AparentPower_4 = AparentPower
        ActivePower_4 = ActivePower
        ReactivePower_4 = ReactivePower             
    elif (i == 5):
        Time5b = datetime.datetime.now()
        delta=(((Time5b - Time5a).microseconds)/1000+((Time5b - Time5a).seconds)*1000)/10000000000
        Energy_5 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_5 += np.abs(ActivePower*delta*2.9)
        Time5a = datetime.datetime.now()
        AparentPower_5 = AparentPower
        ActivePower_5 = ActivePower
        ReactivePower_5 = ReactivePower             
    elif (i == 6):
        Time6b = datetime.datetime.now()
        delta=(((Time6b - Time6a).microseconds)/1000+((Time6b - Time6a).seconds)*1000)/10000000000
        Energy_6 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_6 += np.abs(ActivePower*delta*2.9)
        Time6a = datetime.datetime.now()
        AparentPower_6 = AparentPower
        ActivePower_6 = ActivePower
        ReactivePower_6 = ReactivePower          
    elif (i == 7):
        Time7b = datetime.datetime.now()
        delta=(((Time7b - Time7a).microseconds)/1000+((Time7b - Time7a).seconds)*1000)/10000000000
        Energy_7 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_7 += np.abs(ActivePower*delta*2.9)
        Time7a = datetime.datetime.now()
        AparentPower_7 = AparentPower
        ActivePower_7 = ActivePower
        ReactivePower_7 = ReactivePower            
    elif (i == 8):
        Time8b = datetime.datetime.now()
        delta=(((Time8b - Time8a).microseconds)/1000+((Time8b - Time8a).seconds)*1000)/10000000000
        Energy_8 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_8 += np.abs(ActivePower*delta*2.9)
        Time8a = datetime.datetime.now()
        AparentPower_8 = AparentPower
        ActivePower_8 = ActivePower
        ReactivePower_8 = ReactivePower           
    elif (i == 9):
        Time9b = datetime.datetime.now()
        delta=(((Time9b - Time9a).microseconds)/1000+((Time9b - Time9a).seconds)*1000)/10000000000
        Energy_9 += np.abs(ActivePower*delta*2.9)
        OneHourEnergy_9 += np.abs(ActivePower*delta*2.9)
        Time9a = datetime.datetime.now()
        AparentPower_9 = AparentPower
        ActivePower_9 = ActivePower
        ReactivePower_9 = ReactivePower
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
            plt.title(f'FFT Corriente | DAT: {round(DATCurrent,2)}, FD: {round(FDCurrent
            ,2)} |   cos phi: {round(CosPhi,2)} | phase voltaje CGE : {round(PhaseVoltage,2)} | phase Corriente CGE : {round(PhaseCurrent,2)}',fontdict=font)
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
 

dataVariablesAll=[]
dataCGEAll=[]
data15_1All=[]
dataPanelesAll=[]
dataBateriasAll=[]
dataPanelesDirectaAll=[]

def dataAllVariables():
        #print(f'Guardando lista')
        #print("Temperatura Raspberry:  ",Temp_Raspberry0)
        #print("Uso de CPU Raspberry:  ",cpu_uso)
        dataVariablesAll.insert(1,Temp_Raspberry0)
        dataVariablesAll.insert(2,cpu_uso)
        dataVariablesAll.insert(3,RAM)
        #dataVariablesAll.insert(4,RAM)
"""
def SaveDataCsv_1():
        dataCGEAll.insert(1,round(Vrms,2))
        dataCGEAll.insert(2,round(Irms,2))
        dataCGEAll.insert(3,round(ActivePower,2))
        dataCGEAll.insert(4,round(ReactivePower,2))
        dataCGEAll.insert(5,round(AparentPower1,2))
        dataCGEAll.insert(6,round(FP,2))
        dataCGEAll.insert(7,round(FDCurrent,2))
        dataCGEAll.insert(8,round(DATCurrent,2))
        dataCGEAll.insert(9,round(CosPhi,2))         
        dataCGEAll.insert(10,round(Energy1,2))
        dataCGEAll.insert(11,round(OneHourEnergy1,2))
"""        
def ExcelAllInsert2():        
        data15_1All.insert(1,round(Vrms2,2))
        data15_1All.insert(2,round(Irms2,2))
        data15_1All.insert(3,round(Activa2Fase13,2))
        data15_1All.insert(4,round(Reactiva2Fase13,2))
        data15_1All.insert(5,round(Aparente2Fase13,2))
        data15_1All.insert(6,round(FP21,2))
        data15_1All.insert(7,round(FDCorriente21,2))
        data15_1All.insert(8,round(DATCorriente21,2))
        data15_1All.insert(9,round(cosphi2,2))
        data15_1All.insert(10,round(energy2Fase13,2))
        data15_1All.insert(11,round(energy2Fase13Hour,2))
        
def ExcelAllInsertPaneles():        
        dataPanelesAll.insert(1,round(Vrms3,2))
        dataPanelesAll.insert(2,round(Irms3,2))
        dataPanelesAll.insert(3,round(ActivaPanelesFase12,2))
        dataPanelesAll.insert(4,round(ReactivaPanelesFase12,2))
        dataPanelesAll.insert(5,round(AparentePanelesFase12,2))
        dataPanelesAll.insert(6,round(FPPaneles1,2))
        dataPanelesAll.insert(7,round(FDCorrientePaneles1,2))
        dataPanelesAll.insert(8,round(DATCorrientePaneles1,2))
        dataPanelesAll.insert(9,round(cosphiPaneles,2))
        dataPanelesAll.insert(10,round(energyPanelesFase12,2))
        dataPanelesAll.insert(11,round(energyPanelesFase12Hour,2))


def ExcelAllInsertBaterias():        
        dataBateriasAll.insert(1,round(VoltajeBaterias,2))
        dataBateriasAll.insert(2,round(CorrienteBaterias,2))
        dataBateriasAll.insert(3,round(PotenciaBaterias,2))
        dataBateriasAll.insert(4,round(energyBaterias,2))
        dataBateriasAll.insert(5,round(energyBateriaHora,2))
        
  
def ExcelAllInsertPanelesDC():        
        dataPanelesDirectaAll.insert(1,round(VoltajePanelesDC,2))
        dataPanelesDirectaAll.insert(2,round(CorrientePanelesDC,2))
        dataPanelesDirectaAll.insert(3,round(PotenciaPanelesDC,2))
        dataPanelesDirectaAll.insert(4,round(energyPanelesDC,2))
        dataPanelesDirectaAll.insert(5,round(energyPanelesHoraDC,2)) 
           
"""
'Voltaje', 'Corriente','Potencia Activa','Potencia Reactiva','Potencia Aparente',
'FP','FD','DAT',
"""


Access_1 = 0.0
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
Volt15_1=[]
data15_1=[]
Current15_1=[]
ActivePower15_1=[]
ReactivePower15_1=[]
AparentPower15_1=[]
FP15_Reactive_1=[]
FP15_Inductive_1=[]
FD15_1=[]
DAT15_1=[]
def Maximo15min_1(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,Energy):
    global data15_1
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==15 or basea.minute==30 or basea.minute==45): 
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
                           MaxFPReactive_1=min(FP15_Reactive_1)
                           MeanFPReactive_1=min(FP15_Reactive_1)
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
                    ExcelData15_1()
                    #data15_1.insert(29,energy2Fase13Hour)
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
            ##print(f'Volt152 Despúes: {Volt152}')
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
            print(f'Valor Energia 2 Acumulado: {energy2Fase13} ')
    else:
            excelcreate()
            print("No Existe")

#AbrirExcel()


def VariablesExcel():
       global dataVariablesAll                      
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet1 = workbook["Variables Raspberry"]
       dataVariablesAll.insert(0,datetime.datetime.now())
       sheet1.append(list(dataVariablesAll))
       workbook.save(filename = dest_filename)
       dataVariablesAll=[]

def SaveDataCsv_1(Vrms,Irms,ActivePower_1,ReactivePower_1,AparentPower_1,FP_1,CosPhi_1,FDVoltage_1,FDCurrent_1,DATVoltage_1,DATCurrent_1,Energy_1,OneHourEnergy_1):
       Data_1 = [datetime.datetime.now(),Vrms, Irms, ActivePower_1, ReactivePower_1, AparentPower_1, FP_1, CosPhi_1, FDVoltage_1, FDCurrent_1, DATVoltage_1, DATCurrent_1, Energy_1, OneHourEnergy_1]                    
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet11 = workbook["Var 1"]
       #dataCGEAll.insert(0,datetime.datetime.now())
       sheet11.append(list(dataCGEAll))
       workbook.save(filename = dest_filename)
       Data_1=[]

def ExcelData_15_1():
       global dataCGE                        
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet2 = workbook["CGE Maximos 15 Min"]
       dataCGE.insert(0,datetime.datetime.now())
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

def ExcelData15_115():
       global data15_1
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet3 = workbook["2 Maximos 15 Min"]
       data15_1.insert(0,datetime.datetime.now())
       sheet3.append(list(data15_1))
       #print(f'Data 2: {data15_1}')
       #print("Datos Insertados Correctamente!")
       workbook.save(filename = dest_filename)
       data15_1=[]
     
def ExcelDataPaneles():
       global dataPanelesAll       
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet7 = workbook["Paneles"]
       dataPanelesAll.insert(0,datetime.datetime.now())
       sheet7.append(list(dataPanelesAll))
       #print(f'Numero de filas de paneles: {len(sheet7["FP"])} ')
       #print(f'Data paneles: {dataPaneles}')
       #print("Datos Insertados Correctamente!")
       workbook.save(filename = dest_filename)
       dataPanelesAll=[]

def ExcelDataPaneles15():
       global dataPaneles       
       workbook=openpyxl.load_workbook(filename = dest_filename)
       sheet4 = workbook["Paneles Maximos 15 Min"]
       dataPaneles.insert(0,datetime.datetime.now())
       sheet4.append(list(dataPaneles))
       #print(f'Data paneles: {dataPaneles}')
       #print("Datos Insertados Correctamente!")
       workbook.save(filename = dest_filename)
       dataPaneles=[]





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
        """
        ExcelAllInsertCGE()
        ExcelDataCGE()
        try:
             Maximo15minCGE()
        except OSError as err:
             print("OS error: {0}".format(err))
             continue
        except ValueError:
             print("Could not convert data to an integer.")
             continue
        """
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
                     break
                 np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')   
                 if (len(np_array) == 8402):
                       if (np_array[0] == 11):
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
                           TomaDatos(list_Voltage,list_Current,samplings,i)
                 
                 if (len(np_array)>0 and len(np_array)<=2):
                         global tempESP32
                         global Temp_Raspberry
                         global Temp_Raspberry0
                         global cpu_uso
                         global RAM
                         global RAM1
                         global reinicio
                         Temp_Raspberry0=cpu_temp()
                         cpu_uso=get_cpuload()
                         #str_num = {"value":Temp_Raspberry0,"save":0}
                         Temp_Raspberry = json.dumps(str_num)
                         Ventilador()
                         RAM = psutil.virtual_memory()[2]
                         #dataAllVariables()
                         #VariablesExcel()
                         if (RAM > 93):
                              os.system("sudo reboot")
                         #temphum()
                         #distance()
                         tempESP320 = round(np_array[0],0)
                         #str_num2 = {"value":tempESP320,"save":0}
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

