import requests
import datetime
import json
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
from flask import Flask,render_template, redirect, request
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
import board
import statistics as stat
"""
    0: connection succeeded
    1: connection failed - incorrect protocol version
    2: connection failed - invalid client identifier
    3: connection failed - the broker is not available
    4: connection failed - wrong username or password
    5: connection failed - unauthorized
    6-255: undefined
    """


esp32 = serial.Serial('/dev/ttyUSB0', 230400, timeout=0.5)
esp32.flushInput()


broker = '192.168.100.58'    #mqtt server
port = 1883
dId = '123456789'
passw = 'fklPo4dAXm'
webhook_endpoint = 'http://192.168.100.58:3001/api/getdevicecredentials'


""" 
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

get_mqtt_credentials()
   
def on_disconnect(client, userdata, rc):
    if (rc != 0 and rc != 5):
        print("Unexpected disconnection, will auto-reconnect")
    elif(rc==5):
        print("Getting new credentials!")
        get_mqtt_credentials()
        client.username_pw_set(usernamemqtt, passwordmqtt)
                      
# The callback for when the client receives a CONNACK response from the server.
def on_connected(client, userdata, flags, rc):
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    if rc==0:
        client.connected_flag=True #set flag
        client.subscribe(mqttopic)
        print("connected OK")
        print("rc =",client.connected_flag)
    else:
        print("Bad connection Returned code=",rc)
        client.bad_connection_flag=False
   
       
client = mqtt.Client(str_client_id)   #Creación cliente
client.connect(broker, port)     #Conexión al broker
client.on_disconnect = on_disconnect
client.username_pw_set(usernamemqtt, passwordmqtt)
client.on_connect = on_connected
client.loop_start()
time.sleep(5)



def randomnum():
    num = random.randint(0, 50)
    str_num = {"value":num,"save":1}
    msg = json.dumps(str_num)
    return msg 
       

"""
def setup():
    GPIO.setmode(GPIO.BCM) 
    #GPIO.setup(4,GPIO.OUT)
    #GPIO.setup(24,GPIO.IN)
    GPIO.setup(23, GPIO.OUT)
    #GPIO.setup(8, GPIO.OUT)
    #GPIO.output(8, GPIO.LOW)
    GPIO.setwarnings(False)
    return()


setup()


def cpu_temp():
	thermal_zone = subprocess.Popen(
	    ['cat', '/sys/class/thermal/thermal_zone0/temp'], stdout=subprocess.PIPE)
	out, err = thermal_zone.communicate()
	cpu_temp = int(out.decode())/1000
	return cpu_temp

CPU_temp = 0.0
def Ventilador():
    global EstateVentilador
    CPU_temp = round(cpu_temp(),0)
    #print(f'temp cpu: {CPU_temp}')
    if CPU_temp > 53:
        #print("Ventilador on")
        GPIO.output(23, True)
        EstateVentilador="ON"
    elif CPU_temp <= 38:
        #print("Ventilador off")
        GPIO.output(23, False)
        EstateVentilador="OFF"

def getMaxValues(myList, quantity):
        return(sorted(list(set(myList)), reverse=True)[:quantity]) 
        #print(f'max : {max(myList)}')


def getMinValues(myList, quantity):
        return(sorted(list(set(myList)))[:quantity]) 
        #print(f'max : {max(myList)}')


def EscalaVoltaje(voltaje):
    Escala=getMaxValues(voltaje, 100)
    Escala2=np.mean(Escala)
    #print(f'maximo bits voltaje 1 : {Escala2}')
    if(Escala2>=330):
        newvoltaje=voltaje*0.86
    elif(Escala2<330):
        newvoltaje=voltaje*0.87
    #newvoltaje2=savgol_filter(newvoltaje,51,3)
    Escala3=getMaxValues(newvoltaje, 10)
    Escala4=np.mean(Escala3)
    #print(f'Escala4 : {Escala4}')
    
    return newvoltaje

def EscalaCorriente(corriente):   #bien para valores de bajos amperes en fft
    if(max(corriente)>=850):
        newcorriente=corriente/50   
    elif(max(corriente)<850):
        newcorriente=corriente/140 

    return newcorriente 

def VoltRms(maximovoltaje2):
    vrms=(0.197 + 0.00338*maximovoltaje2 - 0.0000158*(maximovoltaje2**2) + 0.0000000326*(maximovoltaje2**3) - 0.0000000000248*(maximovoltaje2**4))*maximovoltaje2
    print(f'vrms : {vrms}')
    return vrms


def CurrentRms(maximocorriente2):
     irms=(-0.000399 + 0.000137*maximocorriente2 - 0.000000801*(maximocorriente2**2) + 0.00000000214*(maximocorriente2**3) - 0.000000000000218*(maximocorriente2**4))*maximocorriente2
     print(f'irms : {irms}')
     return irms



def VoltajeRms(listVoltage):
    #global vrms
    #print(f'maximo voltaje 2 : {max(listVoltage)}')
    
    listVoltage=savgol_filter(listVoltage,51,10)
    listVoltage=listVoltage*1.055
    #if(max(listVoltage)>=420):
    #    listVoltage=listVoltage*0.75
    #elif(max(listVoltage)<420):
    #    listVoltage=listVoltage*0.79
    N = len(listVoltage)
    Squares = []

    for i in range(0,N,1):    #elevamos al cuadrado cada termino y lo amacenamos
         listsquare = listVoltage[i]*listVoltage[i]
         Squares.append(listsquare)
    
    SumSquares=0
    for i in range(0,N,1):    #Sumatoria de todos los terminos al cuadrado
         SumSquares = SumSquares + Squares[i]

    MeanSquares = (1/N)*SumSquares #Dividimos por N la sumatoria

    vrms=np.sqrt(MeanSquares)
    #print(f'Voltaje RMS : {vrms}')
    

#    return vrms


def CorrienteRms(listCurrent):
    
    #print(f'maximo corriente 2 : {max(listCurrent)}')
    if(max(listCurrent)>1):
        listCurrent=listCurrent*0.7
    elif(max(listCurrent)<1):
        listCurrent=listCurrent*1.5
    
    N = len(listCurrent)
    Squares = []

    for i in range(0,N,1):    #elevamos al cuadrado cada termino y lo amacenamos
         listsquare = listCurrent[i]*listCurrent[i]
         Squares.append(listsquare)
    
    SumSquares=0
    for i in range(0,N,1):    #Sumatoria de todos los terminos al cuadrado
         SumSquares = SumSquares + Squares[i]

    MeanSquares = (1/N)*SumSquares #Dividimos por N la sumatoria

    irms=np.sqrt(MeanSquares)
    #print(f'Corriente RMS : {irms}')
    return irms


DATVoltajeCGE=0.0
phasevoltajeCGE=0.0
FDVoltajeCGE=0.0
DATVoltajePaneles=0.0
phasevoltajePaneles=0.0
FDVoltajePaneles=0.0
DATVoltajeCarga=0.0
phasevoltajeCarga=0.0
FDVoltajeCarga=0.0


def VoltageFFT(list_fftVoltages, samplings,i):
    global j
    j = str(i)
    global DATVoltajeCGE
    global phasevoltajeCGE
    global FDVoltajeCGE
    global DATVoltajePaneles
    global phasevoltajePaneles
    global FDVoltajePaneles
    global DATVoltajeCarga
    global phasevoltajeCarga
    global FDVoltajeCarga
    
    #global FaseArmonicoFundamentalVoltaje
    N = len(list_fftVoltages)
    T = 1 / samplings
    list_fftVoltages -= np.mean(list_fftVoltages)
    datosfft = list_fftVoltages * np.hamming(4096)
    
    yf = np.fft.rfft(datosfft)
    #yf = fft(list_fftVoltages)
    #ejeyfase =  2.0/N * np.abs(yf[:50])
    #index_max2 = np.argmax(ejeyfase[:50])
    #a2 = yf[index_max2]/2048
    xf = fftfreq(N, T)[:N//2]  # tiene un largo de 4096
    #ejey = 2.0/N * np.abs(yf[:N//2])
    if (samplings > 5100):
           #f = interpolate.interp1d(xf, ejey)
           f = interpolate.interp1d(xf, yf[:N//2] )
           xnew = np.arange(0, 2575, 1)  # 2550
           # print(f'largo xnew : {len(xnew)}')
           ynew = f(xnew)
           ejeyabsolut =  2.0/4096 * np.abs(ynew)#ynew
           FD = []
           complejo = []
           real=[]
           imag=[]
           #dccomponent = max(ynew[0:10])
           z=0
           for i in range(45, 2575, 50):
                 a2 = max(ynew[i:i+10])
                 arra = max(ejeyabsolut[i:i+10])
                 complejo.append(a2)
                 #index_max = np.argmax(ejeyabsolut[i-10:i+20])
                 #print(f'a : {a}')
                 #a = ynew[i+index_max]
                 real1 = a2.real
                 real.append(real1)
                 imag1 = a2.imag
                 imag.append(imag1)
                 #radiani = np.arctan(real1/imag1)
                 #degrees = math.degrees(radians)
                 #print(f'index max2 : {i+index_max}')
                 z=z+1
                 FD.append(arra)
                 #print(f'Armonico numero:{z} {i+index_max} + magnitud de {arra} + magnitud2 {abs(ynew[i+index_max])} + forma rectangular de {a} o {a*2/N} y radianes : {np.angle(a)}')
                 #print(f'Armonico corriente numero: {z} =>  {round(arra,3)} + {a2} + .. + {round(radiani,4)})')
                 #print(f'Armonico corriente numero: {z} =>  {round(arra,3)}')
          
           FD2=[]       
           for i in range(0,len(FD)):
               if(FD[i]>(FD[0]/10)):
                   FD2.append(FD[i])
                   
           SumMagnitudEficaz = (np.sum([FD2[0:len(FD2)]]))
           #print(f'Vrms total: {round(SumMagnitudEficaz,2)}')
           Magnitud1 = FD[0]
           #print(f'V rms armonico 1: {round(Magnitud1,2)}')
           #razon=Magnitud1/SumMagnitudEficaz
           #armonico1voltaje=valor*razon
           #print(f'FD Voltaje: {round(FD,2)}')
           #DATVoltaje = np.sqrt((valor**2-armonico1voltaje**2)/(armonico1voltaje**2))

           #sincvoltaje1 = 0
           if(j=="1"):
                 global sincvoltaje1            
                 phasevoltajeCGE = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajeCGE = Magnitud1/SumMagnitudEficaz
                 DATVoltajeCGE = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 sincvoltaje1 = 1
                 #return phasevoltajeCGE,FDvoltajeCGE,DATVoltajeCGE

           #sincvoltaje2 = 0
           if(j=="2"):
                 global sincvoltaje2              
                 phasevoltajePaneles = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajePaneles = Magnitud1/SumMagnitudEficaz
                 DATVoltajePaneles = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 sincvoltaje2 = 1
                 #return phasevoltajePaneles,FDvoltajePaneles,DATVoltajePaneles

           #sincvoltaje3 = 0
           if(j=="3"):
                 global sincvoltaje3
                 phasevoltajeCGE = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajeCarga = Magnitud1/SumMagnitudEficaz
                 DATVoltajeCarga = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 sincvoltaje3 = 1
                 #return phasevoltajeCarga,FDvoltajeCarga,DATVoltajeCarga


DATCorrienteCGE = 0.0
DATCorrientePaneles= 0.0
DATCorrienteCarga= 0.0
FDCorrienteCGE= 0.0
FDCorrientePaneles= 0.0
FDCorrienteCarga= 0.0
FDCorrienteCGE= 0.0
phasecorrienteCGE= 0.0
phasecorrientePaneles= 0.0
phasecorrienteCarga= 0.0
FPCGE= 0.0
cosphiCGE= 0.0
FPPaneles= 0.0
cosphiPaneles= 0.0
FPCarga= 0.0
cosphiCarga= 0.0


def CurrentFFT(list_fftVoltages, samplings, i):
    global DATCorrienteCGE
    global a2
    global FDCorrienteCGE 
    global phasecorrienteCGE
    global FDCorrientePaneles
    global DATCorrientePaneles
    global phasecorrientePaneles
    global FDCorrienteCarga
    global DATCorrienteCarga
    global phasecorrienteCarga
    global FPCGE
    global cosphiCGE
    global FPPaneles
    global cosphiPaneles
    global FPCarga
    global cosphiCarga
    global q
   
    q = str(i)
    N = len(list_fftVoltages)
    T = 1 / samplings
    list_fftVoltages -= np.mean(list_fftVoltages)
    datosfft = list_fftVoltages * np.hamming(4096)#np.kaiser(N,100)
    yf = np.fft.rfft(datosfft)
    #yf=fft(list_fftVoltages)
    #if (g == 1):
    #     print(f'Sampling corriente 1: {round(samplings,2)}')
    
    xf = fftfreq(N, T)[:N//2]
    if (samplings > 5100):
         f = interpolate.interp1d(xf,yf[:N//2])
         xnew = np.arange(0, 2575, 1)
         ynew = f(xnew)
         ejeyabsolut =  2.0/N * np.abs(ynew)
         p = int(i)
         z=0
         FD= []
         #dccomponent = max(ynew[0:10])
         complejo = []
         real=[]
         imag=[]
         #dccomponent = max(ynew[0:10])
         for i in range(45, 2575, 50):
               a2 = max(ynew[i:i+10])
               arra = max(ejeyabsolut[i:i+10])
               complejo.append(a2)
               #index_max = np.argmax(ejeyabsolut[i-10:i+20])
               #print(f'a : {a}')
               #a = ynew[i+index_max]
               real1 = a2.real
               real.append(real1)
               imag1 = a2.imag
               imag.append(imag1)
               #radiani = np.arctan(real1/imag1)
               #degrees = math.degrees(radians)
               #print(f'index max2 : {i+index_max}')
               #z=z+1
               FD.append(arra)
               #print(f'Armonico numero:{z} {i+index_max} + magnitud de {arra} + magnitud2 {abs(ynew[i+index_max])} + forma rectangular de {a} o {a*2/N} y radianes : {np.angle(a)}')
               #print(f'Armonico corriente numero: {z} =>  {round(arra,3)} + {a2} + .. + {round(radiani,4)})')
         FD2=[]       
         for i in range(0,len(FD)):
             if(FD[i]>(FD[0]/10)):
                 FD2.append(FD[i])
                 
         #print(f'FD2: {FD2}')
         #print(f'FD largo: {len(FD)}')
         SumMagnitudEficaz = (np.sum([FD2[0:len(FD2)]]))
         #print(f'Irms total: {round(SumMagnitudEficaz,2)}')
         Magnitud1 = FD[0]
         #print(f'Irms armonico 1: {round(Magnitud1,2)}')
         #razon=Magnitud1/SumMagnitudEficaz
         #armonico1corriente=valor1*razon
         #MagnitudArmonicoFundamentalCorriente=round(thd_array[0],3)
         #fp2=round((armonico1corriente*np.cos(phasevoltaje-phasen))/valor1,2)
         #FaseArmonicoFundamentalCorriente=round(np.angle(complejo[0]),2)
         
         #GradoArmonicoFundamentalCorriente=round(Grados,2)
         if(q=="1"):
             global sincvoltaje1
             FDCorrienteCGE = Magnitud1/SumMagnitudEficaz
             print(f'FD corriente CGE: {FDCorrienteCGE}')
             DATCorrienteCGE = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             phasecorrienteCGE = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje1 == 1):
                 FPCGE=np.cos(phasevoltajeCGE-phasecorrienteCGE)*FDCorrienteCGE
                 cosphiCGE=np.cos(phasevoltajeCGE-phasecorrienteCGE)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 print(f'FP1 cge: {FPCGE}')
                 print(f'cos(phi) cge : {cosphiCGE}')
                 sincvoltaje1=0  
                 #return FPCGE
         #sincvoltaje1=0 
         if(q=="2"):
             global sincvoltaje2
             FDCorrientePaneles = Magnitud1/SumMagnitudEficaz
             #print(f'FDCorrientePaneles : {FDCorrientePaneles }')
             DATCorrientePaneles = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             phasecorrientePaneles = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje2 == 1):
                 FPPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)*FDCorrientePaneles
                 cosphiPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 #print(f'FP1 paneles: {FPPaneles}')
                 #print(f'cos(phi) paneles : {cosphiPaneles}')
                 sincvoltaje2=0  
                 #return FPCGE
         #sincvoltaje2=0 
         if(q=="3"):
             global sincvoltaje3
             FDCorrienteCarga=Magnitud1/SumMagnitudEficaz
             DATCorrienteCarga = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             phasecorrienteCarga = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje3 == 1):
                 FPCarga=np.cos(phasevoltajeCarga-phasecorrienteCarga)*FDCorrienteCarga
                 cosphiCarga=np.cos(phasevoltajeCarga-phasecorrienteCarga)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 #print(f'FP carga : {FPCarga}')
                 #print(f'cos(phi) carga : {cosphiCarga}')
                 sincvoltaje3=0





a = datetime.datetime.now()
b = datetime.datetime.now() 
c = datetime.datetime.now()  
energyCGEFase1 = 0.0
energyPanelesFase1 = 0.0
energyCargaFase1 = 0.0
AparenteCGEFase1 = 0.0
ActivaCGEFase1 = 0.0
ReactivaCGEFase1 = 0.0
AparentePanelesFase1 = 0.0
ActivaPanelesFase1 = 0.0
ReactivaPanelesFase1 = 0.0
AparenteCargaFase1 = 0.0
ActivaCargaFase1 = 0.0
ReactivaCargaFase1 = 0.0



def Potencias(i):
    i = str(i)
    
    if(i=="1"):
          global a
          global energyCGEFase1
          global ActivaCGEFase1
          global AparenteCGEFase1
          global ReactivaCGEFase1
          AparenteCGEFase1 = vrms1*irms1
          print(f'Aparente fase 1 : {AparenteCGEFase1}')
          ActivaCGEFase1= np.abs(vrms1*irms1*cosphiCGE)
          print(f'Activa Fase 1 : {ActivaCGEFase1}')
          ReactivaCGEFase1 = vrms1*irms1*np.sin(phasevoltajeCGE-phasecorrienteCGE)
          print(f'Reactiva Fase 1 : {ReactivaCGEFase1}')
          a2 = datetime.datetime.now()
          delta=(((a2 - a).microseconds)/1000+((a2 - a).seconds)*1000)/10000000000
          energyCGEFase1 += ActivaCGEFase1*delta*2.8
          a = datetime.datetime.now()
    if(i=="2"):
          global b
          global energyPanelesFase1
          global AparentePanelesFase1
          global ActivaPanelesFase1
          global ReactivaPanelesFase1
          AparentePanelesFase1 = vrms*irms
          ActivaPanelesFase1= np.abs(vrms*irms*cosphiPaneles)
          ReactivaPanelesFase1 = vrms*irms*np.sin(phasevoltajePaneles-phasecorrientePaneles)
          b2 = datetime.datetime.now()
          delta=(((b2 - b).microseconds)/1000+((b2 - b).seconds)*1000)/10000000000
          energyPanelesFase1 += ActivaPanelesFase1*delta*2.8
          b = datetime.datetime.now()
    if(i=="3"):
          global c
          global energyCargaFase1 
          global AparenteCargaFase1
          global ActivaCargaFase1
          global ReactivaCargaFase1
          AparenteCargaFase1 = vrms*irms
          ActivaCargaFase1= np.abs(vrms*irms*cosphiCarga)
          ReactivaCargaFase1 = vrms*irms*np.sin(phasevoltajeCarga-phasecorrienteCarga)
          c2 = datetime.datetime.now()
          delta=(((c2 - c).microseconds)/1000+((c2 - c).seconds)*1000)/10000000000
          energyCargaFase1 += ActivaCargaFase1*delta*2.8
          c = datetime.datetime.now()
    


vrms1=0.0
vrms2=0.0
vrms3=0.0
irms1=0.0
irms2=0.0
irms3=0.0
modamaximovoltaje2=[]
modamaximocorriente2=[]
EstateVentilador="OFF"

def received():
           while True:
                  
                  esp32_bytes = esp32.readline()
                  decoded_bytes = str(esp32_bytes[0:len(esp32_bytes)-2].decode("utf-8"))#utf-8
                  np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')
                  #print(f'largo array inicial: {len(np_array)}')
       
                  if (len(np_array) == 8402):
                        if (np_array[0] == 11):
                            global modamaximovoltaje2
                            global modamaximocorriente2
                            global vrms1
                            global irms1
                            global modavoltaje
                            global modacorriente
                            samplings = np_array[-1]
                            list_FPVoltage3 = np_array[0:4200]
                            list_FPCurrent3 = np_array[4201:8400]
                            #print(f'max inicio: {max(list_FPVoltage3)}')
                            sos = signal.butter(10, 3000, 'low', fs=samplings, output='sos')
                            list_FPVoltage2 = signal.sosfilt(sos, list_FPVoltage3)
                            #list_FPVoltage2 = savgol_filter(list_FPVoltage2,len(list_FPVoltage2)-1,))
                            #sos = signal.butter(4, 50, 'low', fs=samplings, output='sos')
                            list_FPCurrent2 = signal.sosfilt(sos, list_FPCurrent3)
                            #print(f'max inicio con filtro: {max(list_FPVoltage2)}')
                            list_FPVoltage = list_FPVoltage2[104:4200]
                            list_FPCurrent = list_FPCurrent2 [103:4200]

                            #Valor dc de Voltaje
                            valoresmaximovoltajesinmedia=getMaxValues(list_FPVoltage, 50)
                            valoresminimovoltajesinmedia=getMinValues(list_FPVoltage, 50)
                            maximovoltaje = np.median(valoresmaximovoltajesinmedia)
                            minimovoltaje = np.median(valoresminimovoltajesinmedia)
                            mediadcvoltaje = (maximovoltaje+minimovoltaje)/2
                            # Valores maximo y minimos de voltaje sin componente continua
                            NoVoltageoffset=list_FPVoltage-mediadcvoltaje
                            maximovoltaje2sinmedia=getMaxValues(NoVoltageoffset, 50)
                            #minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 50)
                            maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                           # vrms1=VoltRms(maximovoltaje2)
                            #minimovoltaje2 = np.median(minimovoltaje2sinmedia)
                            
                            if (len(modamaximovoltaje2)>=20):
                                modavoltaje=np.median(modamaximovoltaje2)
                                vrms1=VoltRms(modavoltaje) 
                                #print(f'maximo voltaje: {maximovoltaje2}')
                                #print(f'minimo voltaje: {minimovoltaje2}')
                                NoVoltageoffset2= EscalaVoltaje(NoVoltageoffset) #Devuelve Array con valores Voltaje peak to peak
                                #NoVoltageoffset2=NoVoltageoffset/1.90
                                #VoltajeRms(NoVoltageoffset2)
                                VoltageFFT(NoVoltageoffset2,samplings,1)
                                #graphVoltage1(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                                #graphFFTV1(NoVoltageoffset2,samplings)
                                #print(f'MODA VOLTAJE: {modavoltaje}')
                                modamaximovoltaje2=[]
                            else:
                                modamaximovoltaje2.append(maximovoltaje2)
                                #print(f'array voltaje: {modamaximovoltaje2}')

                               
                            

                            #print(f'len 1: {len(list_FPVoltage)}')
                                # print(f'maximos{valoresmaximovoltajesinmedia}')
                                # print(f'minimos{valoresminimovoltajesinmedia}')
                                # print(f'samplings 0: {len(list_FPVoltage)}')
                                # print(f'samplings 1: {len(NoVoltageoffset)}')

                            #Valor dc de corriente
                            valoresmaxcorriente=getMaxValues(list_FPCurrent, 50)
                            valoresmincorriente=getMinValues(list_FPCurrent, 50)
                            maximocorriente = np.median(valoresmaxcorriente)
                            minimocorriente = np.median(valoresmincorriente)
        
                            mediadccorriente = (maximocorriente+minimocorriente)/2
                            
                            # Valores maximo y minimos de corriente
                            NoCurrentoffset=list_FPCurrent-mediadccorriente
                            maximocorriente2sinmedia=getMaxValues(NoCurrentoffset, 50)
                            #minimocorriente2sinmedia=getMinValues(NoCurrentoffset, 50)
                            maximocorriente2 = np.median(maximocorriente2sinmedia)
                            #minimocorriente2 = np.median(minimocorriente2sinmedia)
                            
                            if (len(modamaximocorriente2)==20):
                                modacorriente=np.median(modamaximocorriente2)
                                irms1=CurrentRms(modacorriente)
                                #print(f'corriente max: {maximocorriente2 }')
                                #print(f'corriente min: {minimocorriente2 }')
                                NoCurrentoffset2 = EscalaCorriente(NoCurrentoffset)
                                #NoCurrentoffset2 = NoCurrentoffset/125  #210 con res
                                #irms1 = CorrienteRms(NoCurrentoffset2)
                                CurrentFFT(NoCurrentoffset2,samplings,1)
                                #graphCurrent1(NoCurrentoffset2,samplings)
                                #graphFFTI1(NoCurrentoffset2,samplings)
                                #maximo=max(list_FPCurrent[1000:1700])
                                #minimo=min(list_FPCurrent[1000:1700])
                                #diferencia=maximo-minimo
                                #maximo2=max(list_FPCurrent)
                                #escalaI = valor1*np.sqrt(2) / maximo2
                                #listEscalaI=list_FPCurrent*escalaI
                                #samplings = np_array[-1]
                                #graphVoltageCurrent(NoVoltageoffset,NoCurrentoffset,samplings)
                                print(f'MODA CORRIENTE: {modacorriente}')
                                Potencias(1)
                                modamaximocorriente2=[]
                            else:
                                modamaximocorriente2.append(maximocorriente2)
                            #    print(f'array corriente: {modamaximocorriente2}')
                            
                                
                            
                            
                        if (np_array[0] == 22):
                            global vrms2
                            global irms2
                            samplings = np_array[-1]
                            list_FPVoltage3 = np_array[0:4200]
                            list_FPCurrent3 = np_array[4201:8400]
                            
                            sos = signal.butter(10, 3000, 'low', fs=samplings, output='sos')
                            list_FPVoltage2 = signal.sosfilt(sos, list_FPVoltage3)
                            #list_FPVoltage2 = savgol_filter(list_FPVoltage2,len(list_FPVoltage2)-1,))
                            #sos = signal.butter(4, 50, 'low', fs=samplings, output='sos')
                            list_FPCurrent2 = signal.sosfilt(sos, list_FPCurrent3)
                            
                            list_FPVoltage = list_FPVoltage2[104:4200]
                            list_FPCurrent = list_FPCurrent2 [103:4200]

                            #Valor dc de Voltaje
                            valoresmaximovoltajesinmedia=getMaxValues(list_FPVoltage, 20)
                            valoresminimovoltajesinmedia=getMinValues(list_FPVoltage, 20)
                            maximovoltaje = np.median(valoresmaximovoltajesinmedia)
                            minimovoltaje = np.median(valoresminimovoltajesinmedia)
                            mediadcvoltaje = (maximovoltaje+minimovoltaje)/2
                            # Valores maximo y minimos de voltaje sin componente continua
                            NoVoltageoffset=list_FPVoltage-mediadcvoltaje
                            maximovoltaje2sinmedia=getMaxValues(NoVoltageoffset, 20)
                            minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 20)
                            maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                            minimovoltaje2 = np.median(minimovoltaje2sinmedia)
                            #print(f'maximo voltaje{maximovoltaje2}')
                            #print(f'maximo voltaje{minimovoltaje2}')
                            NoVoltageoffset2 = EscalaVoltaje(NoVoltageoffset)
                            #NoVoltageoffset2=NoVoltageoffset/1.90

                            #print(f'len 1: {len(list_FPVoltage)}')
                                # print(f'maximos{valoresmaximovoltajesinmedia}')
                                # print(f'minimos{valoresminimovoltajesinmedia}')
                                # print(f'samplings 0: {len(list_FPVoltage)}')
                                # print(f'samplings 1: {len(NoVoltageoffset)}')

                            #Valor dc de corriente
                            valoresmaxcorriente=getMaxValues(list_FPCurrent, 20)
                            valoresmincorriente=getMinValues(list_FPCurrent, 20)
                            maximocorriente = np.median(valoresmaxcorriente)
                            minimocorriente = np.median(valoresmincorriente)
        
                            mediadccorriente = (maximocorriente+minimocorriente)/2
                            
                            # Valores maximo y minimos de corriente
                            NoCurrentoffset=list_FPCurrent-mediadccorriente
                            maximocorriente2sinmedia=getMaxValues(NoCurrentoffset, 20)
                            minimocorriente2sinmedia=getMinValues(NoCurrentoffset, 20)
                            maximocorriente2 = np.median(maximocorriente2sinmedia)
                            minimocorriente2 = np.median(minimocorriente2sinmedia)
                            #print(f'corriente max: {maximocorriente2 }')
                            #print(f'corriente min: {minimocorriente2 }')
                            NoCurrentoffset2 = EscalaCorriente(NoCurrentoffset)
                            #NoCurrentoffset2 = NoCurrentoffset/125  #210 con res


                            vrms2=VoltajeRms(NoVoltageoffset2)
                            VoltageFFT(NoVoltageoffset2,samplings,2)
                            #graphVoltage2(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                            #graphFFTV2(NoVoltageoffset2,samplings)
                            
                            
                            irms2 = CorrienteRms(NoCurrentoffset2)
                            CurrentFFT(NoCurrentoffset2,samplings,2)
                            #graphCurrent2(NoCurrentoffset2,samplings)
                            #graphFFTI2(NoCurrentoffset2,samplings)
                            #maximo=max(list_FPCurrent[1000:1700])
                            #minimo=min(list_FPCurrent[1000:1700])
                            #diferencia=maximo-minimo
                            #maximo2=max(list_FPCurrent)
                            #escalaI = valor1*np.sqrt(2) / maximo2
                            #listEscalaI=list_FPCurrent*escalaI
                            #samplings = np_array[-1]
                            #graphVoltageCurrent(NoVoltageoffset,NoCurrentoffset,samplings)
                            Potencias(2,irms2,vrms2)
                         #   print(f'samplings 2: {samplings}')
                            #FP(list_FPVoltage, list_FPCurrent, i=1)
                        if (np_array[0] == 33):
                            global vrms3
                            global irms3
                            samplings = np_array[-1]
                            list_FPVoltage3 = np_array[0:4200]
                            list_FPCurrent3 = np_array[4201:8400]
                            
                            sos = signal.butter(10, 3000, 'low', fs=samplings, output='sos')
                            list_FPVoltage2 = signal.sosfilt(sos, list_FPVoltage3)
                            #list_FPVoltage2 = savgol_filter(list_FPVoltage2,len(list_FPVoltage2)-1,))
                            #sos = signal.butter(4, 50, 'low', fs=samplings, output='sos')
                            list_FPCurrent2 = signal.sosfilt(sos, list_FPCurrent3)
                            
                            list_FPVoltage = list_FPVoltage2[104:4200]
                            list_FPCurrent = list_FPCurrent2[103:4200]

                            #Valor dc de Voltaje
                            valoresmaximovoltajesinmedia=getMaxValues(list_FPVoltage, 20)
                            valoresminimovoltajesinmedia=getMinValues(list_FPVoltage, 20)
                            maximovoltaje = np.median(valoresmaximovoltajesinmedia)
                            minimovoltaje = np.median(valoresminimovoltajesinmedia)
                            mediadcvoltaje = (maximovoltaje+minimovoltaje)/2
                            # Valores maximo y minimos de voltaje sin componente continua
                            NoVoltageoffset=list_FPVoltage-mediadcvoltaje
                            maximovoltaje2sinmedia=getMaxValues(NoVoltageoffset, 20)
                            minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 20)
                            maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                            minimovoltaje2 = np.median(minimovoltaje2sinmedia)
                            #print(f'maximo voltaje{maximovoltaje2}')
                            #print(f'maximo voltaje{minimovoltaje2}')
                            NoVoltageoffset2 = EscalaVoltaje(NoVoltageoffset)
                            #NoVoltageoffset2=NoVoltageoffset/1.90

                            #print(f'len 1: {len(list_FPVoltage)}')
                                # print(f'maximos{valoresmaximovoltajesinmedia}')
                                # print(f'minimos{valoresminimovoltajesinmedia}')
                                # print(f'samplings 0: {len(list_FPVoltage)}')
                                # print(f'samplings 1: {len(NoVoltageoffset)}')

                            #Valor dc de corriente
                            valoresmaxcorriente=getMaxValues(list_FPCurrent, 20)
                            valoresmincorriente=getMinValues(list_FPCurrent, 20)
                            maximocorriente = np.median(valoresmaxcorriente)
                            minimocorriente = np.median(valoresmincorriente)
        
                            mediadccorriente = (maximocorriente+minimocorriente)/2
                            
                            # Valores maximo y minimos de corriente
                            NoCurrentoffset=list_FPCurrent-mediadccorriente
                            maximocorriente2sinmedia=getMaxValues(NoCurrentoffset, 20)
                            minimocorriente2sinmedia=getMinValues(NoCurrentoffset, 20)
                            maximocorriente2 = np.median(maximocorriente2sinmedia)
                            minimocorriente2 = np.median(minimocorriente2sinmedia)
                            #print(f'corriente max: {maximocorriente2 }')
                            #print(f'corriente min: {minimocorriente2 }')
                            NoCurrentoffset2 = EscalaCorriente(NoCurrentoffset)
                            #NoCurrentoffset2 = NoCurrentoffset/125  #210 con res


                            vrms3=VoltajeRms(NoVoltageoffset2)
                            VoltageFFT(NoVoltageoffset2,samplings,3)
                            #graphVoltage3(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                            #graphFFTV3(NoVoltageoffset2,samplings)
                            
                            
                            irms3 = CorrienteRms(NoCurrentoffset2)
                            CurrentFFT(NoCurrentoffset2,samplings,3)
                            #graphCurrent3(NoCurrentoffset2,samplings)
                            #graphFFTI3(NoCurrentoffset2,samplings)
                            #maximo=max(list_FPCurrent[1000:1700])
                            #minimo=min(list_FPCurrent[1000:1700])
                            #diferencia=maximo-minimo
                            #maximo2=max(list_FPCurrent)
                            #escalaI = valor1*np.sqrt(2) / maximo2
                            #listEscalaI=list_FPCurrent*escalaI
                            #samplings = np_array[-1]
                            #graphVoltageCurrent(NoVoltageoffset,NoCurrentoffset,samplings)
                            Potencias(3,irms3,vrms3)
                       #     print(f'samplings 3: {samplings}')
                            #FP(list_FPVoltage, list_FPCurrent, i=1)
                    
                  if (len(np_array)>0 and len(np_array)<=2):
                          global tempESP32
                          #global EstateVentilador
                          Temp_Raspberry=cpu_temp()
                          #str(EstateVentilador)
                          Ventilador()
                          #temphum()
                          #distance()
                          tempESP32 = round(np_array[0],0)
                          print(f'tempESP32: {tempESP32}')
                          print(f'tempRaspberry: {Temp_Raspberry}')
                          print(f'Estado Vendilador: {EstateVentilador}')
        


"""
def publish(client):
    
    #for i in range(0,len(data["variables"])):
    #    if(data["variables"][i]["variableType"]=="output"):
    #        continue
        str_variable = data["variables"][0]["variable"]
        #print("data:",str_variable)
        topic1 = topicmqtt + str_variable + "/sdata"
        msg=randomnum()
        result = client.publish(topic1, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic1}`")
        else:
            print(f"Failed to send message to topic {topic1}")
        
        #str_variable = data["variables"][1]["variable"]
        #print("data:",str_variable)
        #topic1 = topicmqtt + str_variable + "/sdata"
        #msg=randomnum2()
        #result = client.publish(topic1, msg)
        # result: [0, 1]
        #status = result[0]
        #if status == 0:
        #    print(f"Send `{msg}` to topic `{topic1}`")
        #else:
        #    print(f"Failed to send message to topic {topic1}")
"""

""" 
while client.connected_flag: 
    #print("In loop")
    publish(client)
    #time.sleep(5)
"""   


if __name__ == '__main__':
    received()
    #t = threading.Thread(target=received)
    #t.daemon = True
    #t.start()
