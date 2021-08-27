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


broker = '192.168.100.122'    #mqtt server
port = 1883
dId = '12344321'
passw = 'yFJMESnzxl'
webhook_endpoint = 'http://192.168.100.122:3001/api/getdevicecredentials'


 
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





def cpu_temp():
	thermal_zone = subprocess.Popen(
	    ['cat', '/sys/class/thermal/thermal_zone0/temp'], stdout=subprocess.PIPE)
	out, err = thermal_zone.communicate()
	cpu_temp = int(out.decode())/1000
	return cpu_temp

CPU_temp = 0.0
def Ventilador():
    global CPU_temp
    CPU_temp = round(cpu_temp(),0)
    #print(f'temp cpu: {CPU_temp}')
    if CPU_temp > 50:
        #print("Ventilador on")
        GPIO.output(23, True)
    elif CPU_temp <= 40:
        #print("Ventilador off")
        GPIO.output(23, False)

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

vrms=0.0
def VoltRms(maximovoltaje2):
    global vrms
    vrmsx=(0.197 + 0.00338*maximovoltaje2 - 0.0000158*(maximovoltaje2**2) + 0.0000000326*(maximovoltaje2**3) - 0.0000000000248*(maximovoltaje2**4))*maximovoltaje2
    print(f'vrms : {vrmsx}')
    str_num = {"value":vrmsx,"save":1}
    vrms = json.dumps(str_num)
    return vrmsx

irms=0.0
def CurrentRms(maximocorriente2):
     global irms
     irmsx=(-0.000399 + 0.000137*maximocorriente2 - 0.000000801*(maximocorriente2**2) + 0.00000000214*(maximocorriente2**3) - 0.000000000000218*(maximocorriente2**4))*maximocorriente2
     print(f'irms : {irmsx}')
     str_num = {"value":irmsx,"save":1}
     irms = json.dumps(str_num)
     return irmsx



vrms0=0.0
def VoltajeRms(listVoltage):
    global vrms0
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

    vrms0=np.sqrt(MeanSquares)
   
    #print(f'Voltaje RMS : {vrms0}')
    

#    return vrms


irms0=0.0
def CorrienteRms(listCurrent):
    global irms0
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

    irms0=np.sqrt(MeanSquares)
    
    #print(f'Corriente RMS : {irms0}')
    return irms0


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
                 str_num = {"value":FDVoltajeCGE,"save":1}
                 FDVoltajeCGE = json.dumps(str_num)
                 DATVoltajeCGE= np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 #print(f'DAT Voltaje CGE: {round(DATVoltajeCGE,2)}')
                 str_num = {"value":DATVoltajeCGE,"save":1}
                 DATVoltajeCGE = json.dumps(str_num)
                 sincvoltaje1 = 1
                 
                 #return phasevoltajeCGE,FDvoltajeCGE,DATVoltajeCGE

           #sincvoltaje2 = 0
           if(j=="2"):
                 global sincvoltaje2              
                 phasevoltajePaneles = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajePaneles = Magnitud1/SumMagnitudEficaz
                 str_num = {"value":FDVoltajePaneles,"save":1}
                 FDVoltajePaneles = json.dumps(str_num)
                 DATVoltajePaneles = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 str_num = {"value":DATVoltajePaneles,"save":1}
                 DATVoltajePaneles = json.dumps(str_num)
                 sincvoltaje2 = 1
                 #return phasevoltajePaneles,FDvoltajePaneles,DATVoltajePaneles

           #sincvoltaje3 = 0
           if(j=="3"):
                 global sincvoltaje3
                 phasevoltajeCarga = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajeCarga = Magnitud1/SumMagnitudEficaz
                 str_num = {"value":FDVoltajeCarga,"save":1}
                 FDVoltajeCarga = json.dumps(str_num)
                 DATVoltajeCarga = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 str_num = {"value":DATVoltajeCarga,"save":1}
                 DATVoltajeCarga = json.dumps(str_num)
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
             FDCorrienteCGE1 = Magnitud1/SumMagnitudEficaz
             str_num = {"value":FDCorrienteCGE1,"save":1}
             FDCorrienteCGE = json.dumps(str_num)
             DATCorrienteCGE = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             str_num2 = {"value":DATCorrienteCGE,"save":1}
             DATCorrienteCGE = json.dumps(str_num2)
             #print(f'DAT corriente CGE: {DATCorrienteCGE}')
             phasecorrienteCGE = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje1 == 1):
                 FPCGE=np.cos(phasevoltajeCGE-phasecorrienteCGE)*FDCorrienteCGE1
                 cosphiCGE=np.cos(phasevoltajeCGE-phasecorrienteCGE)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 #print(f'FP1 cge: {FPCGE}')
                 str_num3 = {"value":FPCGE,"save":1}
                 FPCGE = json.dumps(str_num3)
                 #print(f'cos(phi) cge : {cosphiCGE}')
                 sincvoltaje1=0  
                 #return FPCGE
         #sincvoltaje1=0 
         if(q=="2"):
             global sincvoltaje2
             FDCorrientePaneles = Magnitud1/SumMagnitudEficaz
             str_num = {"value":FDCorrientePaneles,"save":1}
             FDCorrientePaneles = json.dumps(str_num)
             #print(f'FDCorrientePaneles : {FDCorrientePaneles }')
             DATCorrientePaneles = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             str_num2 = {"value":DATCorrientePaneles,"save":1}
             DATCorrientePaneles = json.dumps(str_num2)
             phasecorrientePaneles = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje2 == 1):
                 FPPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)*FDCorrientePaneles
                 cosphiPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 #print(f'FP1 paneles: {FPPaneles}')
                 #print(f'cos(phi) paneles : {cosphiPaneles}')
                 str_num = {"value":FPPaneles,"save":1}
                 FPPaneles = json.dumps(str_num)
                 sincvoltaje2=0  
                 #return FPCGE
         #sincvoltaje2=0 
         if(q=="3"):
             global sincvoltaje3
             FDCorrienteCarga=Magnitud1/SumMagnitudEficaz
             str_num = {"value":FDCorrientePaneles,"save":1}
             FDCorrienteCarga = json.dumps(str_num)
             DATCorrienteCarga = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             str_num2 = {"value":DATCorrienteCarga,"save":1}
             DATCorrienteCarga = json.dumps(str_num2)
             phasecorrienteCarga = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje3 == 1):
                 FPCarga=np.cos(phasevoltajeCarga-phasecorrienteCarga)*FDCorrienteCarga
                 cosphiCarga=np.cos(phasevoltajeCarga-phasecorrienteCarga)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 #print(f'FP carga : {FPCarga}')
                 #print(f'cos(phi) carga : {cosphiCarga}')
                 str_num = {"value":FPCarga,"save":1}
                 FPCarga = json.dumps(str_num)
                 sincvoltaje3=0





a = datetime.datetime.now()
b = datetime.datetime.now() 
c = datetime.datetime.now()  
energyCGEFase01 = 0.0
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



def Potencias(i,irms,vrms):
    i = str(i)
    
    if(i=="1"):
          global a
          global energyCGEFase01
          global ActivaCGEFase1
          global AparenteCGEFase1
          global ReactivaCGEFase1
          AparenteCGEFase1 = vrms0*irms0
          ActivaCGEFase1= np.abs(vrms0*irms0*cosphiCGE)
          ReactivaCGEFase1 = vrms0*irms0*np.sin(phasevoltajeCGE-phasecorrienteCGE)
          a2 = datetime.datetime.now()
          delta=(((a2 - a).microseconds)/1000+((a2 - a).seconds)*1000)/10000000000
          energyCGEFase01 += ActivaCGEFase1*delta*2.8
          a = datetime.datetime.now()

          #print(f'Activa Fase 1: {round(ActivaCGEFase01,2)}')
          #print(f'Aparente Fase 1: {round(AparenteCGEFase1,2)}')
          str_num = {"value":ActivaCGEFase1,"save":1}
          str_num2 = {"value":ReactivaCGEFase1,"save":1}
          str_num3 = {"value":AparenteCGEFase1,"save":1}
          str_num4 = {"value":energyCGEFase01,"save":1}
          ActivaCGEFase1 = json.dumps(str_num)
          AparenteCGEFase1 = json.dumps(str_num3)
          ReactivaCGEFase1 = json.dumps(str_num2)
          energyCGEFase1 = json.dumps(str_num4)
    if(i=="2"):
          global b
          global energyPanelesFase1
          global AparentePanelesFase1
          global ActivaPanelesFase1
          global ReactivaPanelesFase1
          AparentePanelesFase1 = vrms0*irms0
          ActivaPanelesFase1= np.abs(vrms0*irms0*cosphiPaneles)
          ReactivaPanelesFase1 = vrms0*irms0*np.sin(phasevoltajePaneles-phasecorrientePaneles)
          b2 = datetime.datetime.now()
          delta=(((b2 - b).microseconds)/1000+((b2 - b).seconds)*1000)/10000000000
          energyPanelesFase1 += ActivaPanelesFase1*delta*2.8
          b = datetime.datetime.now()

          str_num = {"value":ActivaPanelesFase1,"save":1}
          str_num2 = {"value":ReactivaPanelesFase1,"save":1}
          str_num3 = {"value":AparentePanelesFase1,"save":1}
          str_num4 = {"value":energyPanelesFase1,"save":1}
          ActivaPanelesFase1 = json.dumps(str_num)
          AparentePanelesFase1 = json.dumps(str_num3)
          ReactivaPanelesFase1 = json.dumps(str_num2)
          energyPanelesFase1 = json.dumps(str_num4)
    if(i=="3"):
          global c
          global energyCargaFase1 
          global AparenteCargaFase1
          global ActivaCargaFase1
          global ReactivaCargaFase1
          AparenteCargaFase1 = vrms0*irms0
          ActivaCargaFase1= np.abs(vrms0*irms0*cosphiCarga)
          ReactivaCargaFase1 = vrms0*irms0*np.sin(phasevoltajeCarga-phasecorrienteCarga)
          c2 = datetime.datetime.now()
          delta=(((c2 - c).microseconds)/1000+((c2 - c).seconds)*1000)/10000000000
          energyCargaFase1 += ActivaCargaFase1*delta*2.8
          c = datetime.datetime.now()

          str_num = {"value":ActivaCargaFase1,"save":1}
          str_num2 = {"value":ReactivaCargaFase1,"save":1}
          str_num3 = {"value":AparenteCargaFase1,"save":1}
          str_num4 = {"value":energyCargaFase1,"save":1}
          ActivaCargaFase1 = json.dumps(str_num)
          AparenteCargaFase1 = json.dumps(str_num3)
          ReactivaCargaFase1 = json.dumps(str_num2)
          energyCargaFase1 = json.dumps(str_num4)
    


vrms1=0.0
vrms2=0.0
vrms3=0.0
irms1=0.0
irms2=0.0
irms3=0.0
modamaximovoltaje2=[]
modamaximocorriente2=[]

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
                            minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 50)
                            maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                            minimovoltaje2 = np.median(minimovoltaje2sinmedia)

                            if (len(modamaximovoltaje2)==10):
                                modavoltaje=np.median(modamaximovoltaje2)
                                #print(f'MODA VOLTAJE: {modavoltaje}')
                                vrms1=VoltRms(modavoltaje) 
                                modamaximovoltaje2=[]
                            else:
                                modamaximovoltaje2.append(maximovoltaje2)
                                #print(f'array voltaje: {modamaximovoltaje2}')
                            #print(f'maximo voltaje con get max value : {maximovoltaje}')
                            #print(f'maximo voltaje{minimovoltaje2}')
                            NoVoltageoffset2= EscalaVoltaje(NoVoltageoffset) #Devuelve Array con valores Voltaje peak to peak
                            #NoVoltageoffset2=NoVoltageoffset/1.90

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
                            minimocorriente2sinmedia=getMinValues(NoCurrentoffset, 50)
                            maximocorriente2 = np.median(maximocorriente2sinmedia)

                            if (len(modamaximocorriente2)==10):
                                modacorriente=np.median(modamaximocorriente2)
                                irms1=CurrentRms(modacorriente)
                               # print(f'MODA CORRIENTE: {modacorriente}')
                                modamaximocorriente2=[]
                            else:
                                modamaximocorriente2.append(maximocorriente2)
                            #    print(f'array corriente: {modamaximocorriente2}')
                            #minimocorriente2 = np.median(minimocorriente2sinmedia)
                            #print(f'corriente max: {maximocorriente2 }')
                            #print(f'corriente min: {minimocorriente2 }')
                            NoCurrentoffset2 = EscalaCorriente(NoCurrentoffset)
                            #NoCurrentoffset2 = NoCurrentoffset/125  #210 con res


                            VoltajeRms(NoVoltageoffset2)
                            VoltageFFT(NoVoltageoffset2,samplings,1)
                            #graphVoltage1(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                            #graphFFTV1(NoVoltageoffset2,samplings)
                            
                            
                            irms1 = CorrienteRms(NoCurrentoffset2)
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
                            Potencias(1,irms1,vrms1)
                            #print(f'samplings 1: {samplings}')
                            #FP(list_FPVoltage, list_FPCurrent, i=1)
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
                            print(f'samplings 2: {samplings}')
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
                            print(f'samplings 3: {samplings}')
                            #FP(list_FPVoltage, list_FPCurrent, i=1)
                    
                  if (len(np_array)>0 and len(np_array)<=2):
                          global tempESP32
                          #Ventilador()
                          #temphum()
                          #distance()
                          tempESP32 = round(np_array[0],0)
                          print(f'array: {np_array}')
                  if  (client.connected_flag == True): 
                          publish(client)


b1=time.time()
c1=time.time()
d1=time.time()
e21=time.time()
f1=time.time()
g1=time.time()
h1=time.time()
j1=time.time()
k1=time.time()
#varsLastSend=[b,c,d,e]

def publish(client): 
        global b1, c1 ,d1, e21, f1 ,g1 ,h1 , j1, k1
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
                     result = client.publish(topic1, irms)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send irms: `{irms}` to topic `{topic1}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic1}")
        
                   
            if(i["variableFullName"]=="Voltaje-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - c1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     c1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, vrms)
                     status = result[0]
                     if status == 0:
                         print(f"Send vrms: `{vrms}` to topic `{topic2}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic2}")
            
            if(i["variableFullName"]=="Potencia-Reactiva-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - d1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     d1=time.time()
                     str_variable3 = i["variable"]
                     topic3 = topicmqtt + str_variable3 + "/sdata"
                     result = client.publish(topic3, ReactivaCGEFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send pot activa: `{ReactivaCGEFase1}` to topic `{topic3}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic3}")
            
            if(i["variableFullName"]=="Pot-Activa-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - e21 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     e21=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, ActivaCGEFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send FP: `{ActivaCGEFase1}` to topic `{topic4}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic4}")
            
            if(i["variableFullName"]=="Energia-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - f1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     f1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energyCGEFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT: `{energyCGEFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="FP-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - g1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     g1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FPCGE)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT: `{FPCGE}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="FD-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - h1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     h1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FDCorrienteCGE)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT: `{FDCorrienteCGE}` to topic `{topic5}` con freq: {freq}")
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
                         print(f"Send DAT: `{DATCorrienteCGE}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="Pot-Aparente-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - k1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     k1=time.time()
                     str_variable = i["variable"]
                     topic5= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, AparenteCGEFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT: `{AparenteCGEFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            


"""
while client.connected_flag: 
    #print("In loop")
    #received()
    publish(client)
    #time.sleep(5)
"""   


if __name__ == '__main__':
    received()
    #t = threading.Thread(target=received)
    #t.daemon = True
    #t.start()

