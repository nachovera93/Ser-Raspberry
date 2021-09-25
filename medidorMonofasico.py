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
#import board
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font
import openpyxl
import smtplib, ssl
import getpass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import datetime
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

horasetup=datetime.datetime.now()
print(f'Hora de comienzo: {horasetup}')

broker = '18.228.175.193'    #mqtt server
port = 1883
dId = '123456'
passw = 'zDu9VeuECs'
webhook_endpoint = 'http://18.228.175.193:3001/api/getdevicecredentials'


Lugar="Santa Cristina"
username = "empresasserspa@gmail.com"
password = "empresasserspa"
destinatario = "empresasserspa@gmail.com"
destinatario2 = "demetrio.vera@serm.cl"

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

archivo = "prueba_Medidor.xlsx"

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

def SendEmail():
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
          server.login(username,password)
          print("Sesión Iniciada Correctamente !")
          #server.sendmail(username, destinatario, mensaje)
          server.sendmail(username, destinatario, text)
          server.sendmail(username, destinatario2, text)
          print("Mensaje Enviado Correctamente !")


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

   

get_mqtt_credentials()     
client = mqtt.Client(str_client_id)   #Creación cliente
client.connect(broker, port)     #Conexión al broker
client.on_disconnect = on_disconnect
client.username_pw_set(usernamemqtt, passwordmqtt)
client.on_connect = on_connected
client.loop_start()
  

def setup():
    GPIO.setmode(GPIO.BCM) 
    GPIO.setup(4,GPIO.OUT)
    GPIO.setup(24,GPIO.IN)
    GPIO.setup(23, GPIO.OUT)
    GPIO.setup(8, GPIO.OUT)
    GPIO.output(8, GPIO.LOW)
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
EstateVentilador="OFF"

def Ventilador():
    global EstateVentilador
    CPU_temp = round(cpu_temp(),0)
    #print(f'temp cpu: {CPU_temp}')
    if CPU_temp > 53:
        #print("Ventilador on")
        GPIO.output(23, True)
        #EstateVentilador0="ON"
        str_num = {"value":"ON","save":1}
        EstateVentilador = json.dumps(str_num)
    elif CPU_temp <= 38:
        #print("Ventilador off")
        GPIO.output(23, False)
        #EstateVentilador0="OFF"
        str_num = {"value":"OFF","save":1}
        EstateVentilador = json.dumps(str_num)

def getMaxValues(myList, quantity):
        return(sorted(list(set(myList)), reverse=True)[:quantity]) 
        #print(f'max : {max(myList)}')


def getMinValues(myList, quantity):
        return(sorted(list(set(myList)))[:quantity]) 
        #print(f'max : {max(myList)}')


vrms=0.0
def VoltRms(maximovoltaje2):
    if(maximovoltaje2<13):
        vrms=0
        print(f'vrms : {vrms}')
        return vrms
    vrms=maximovoltaje2*0.65
    #vrms=(-1.16 + 0.179*(maximovoltaje2) +  -0.00718*(maximovoltaje2**2) + 0.000155*(maximovoltaje2**3) + -0.00000203*(maximovoltaje2**4) + 0.0000000168*(maximovoltaje2**5) + -0.0000000000912*(maximovoltaje2**6) + 0.00000000000032*(maximovoltaje2**7) + -0.000000000000000703*(maximovoltaje2**8) + 0.000000000000000000875*(maximovoltaje2**9) + -0.000000000000000000000472*(maximovoltaje2**10))*maximovoltaje2
    #print(f'vrms : {vrms}')
    return vrms

irms=0.0
def CurrentRms(maximocorriente2):
    #irms=(-0.0248 + 0.00402*maximocorriente2 - 0.000176*(maximocorriente2**2) + 0.00000392*(maximocorriente2**3) - 0.000000046*(maximocorriente2**4) + 0.000000000284*(maximocorriente2**5) - 0.00000000000069*(maximocorriente2**6))*maximocorriente2
    #print(f'irms : {irms}')
    if(maximocorriente2>430):
         irms = maximocorriente2*0.0133
    else:
         irms=(0.00435 + 0.000298*maximocorriente2 - 0.00000349*(maximocorriente2**2) + 0.0000000176*(maximocorriente2**3) - 0.0000000000398*(maximocorriente2**4) + 0.0000000000000332*(maximocorriente2**5))*maximocorriente2
    
    return irms



vrms0=0.0
def VoltajeRms(listVoltage):
    global vrms0
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
    return vrms0


irms0=0.0
def CorrienteRms(listCurrent):
    global irms0
    
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
                   
           SumMagnitudEficaz = (np.sum([FD[0:len(FD)]]))
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
                 DATVoltajeCGE1= np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 print(f'DAT Voltaje CGE: {round(DATVoltajeCGE1,2)}')
                 str_num = {"value":DATVoltajeCGE1,"save":1}
                 DATVoltajeCGE = json.dumps(str_num)
                 sincvoltaje1 = 1
                 
                 #return phasevoltajeCGE,FDvoltajeCGE,DATVoltajeCGE

           #sincvoltaje2 = 0
           if(j=="3"):
                 global sincvoltaje2              
                 phasevoltajePaneles = np.arctan(real[0]/(imag[0]))
                 print(f'phase voltaje paneles: {round(phasevoltajePaneles,2)}')
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajePaneles = Magnitud1/SumMagnitudEficaz
                 str_num = {"value":FDVoltajePaneles,"save":1}
                 FDVoltajePaneles = json.dumps(str_num)
                 DATVoltajePaneles1 = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 print(f'DAT Voltaje Paneles: {round(DATVoltajePaneles1,2)}')
                 str_num = {"value":DATVoltajePaneles1,"save":1}
                 DATVoltajePaneles = json.dumps(str_num)
                 sincvoltaje2 = 1
                 #return phasevoltajePaneles,FDvoltajePaneles,DATVoltajePaneles

           #sincvoltaje3 = 0
           if(j=="2"):
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
FDCorrienteCarga=0.0
DATCorrienteCarga=0.0
FDCorrientePaneles= 0.0
FDCorrienteCarga= 0.0
FDCorrienteCGE= 0.0
phasecorrienteCGE= 0.0
phasecorrientePaneles= 0.0
phasecorrienteCarga= 0.0
FPCGE= 0.0
FPCGE0= 0.0
cosphiCGE= 0.0
FPPaneles= 0.0
cosphiPaneles= 0.0
FPCarga= 0.0
cosphiCarga= 0.0
FDCorrienteCGE1=0.0
DATCorrienteCGE1=0.0
FPCarga1=0.0
FPPaneles1 = 0.0
FDCorrientePaneles1 = 0.0
DATCorrientePaneles1 = 0.0  


def CurrentFFT(list_fftVoltages, samplings, i,irms):
    global DATCorrienteCGE
    global DATCorrienteCGE1
    global a2
    global FDCorrienteCGE 
    global FDCorrienteCGE1
    global phasecorrienteCGE
    global FDCorrientePaneles
    global DATCorrientePaneles
    global phasecorrientePaneles
    global FDCorrienteCarga
    global DATCorrienteCarga
    global FDCorrienteCarga1
    global DATCorrienteCarga1
    global phasecorrienteCarga
    global FPCGE
    global FPCarga1
    global FPCGE0
    global cosphiCGE
    global FPPaneles
    global cosphiPaneles
    global FPCarga
    global cosphiCarga
    global q
    global FPPaneles1
    global FDCorrientePaneles1
    global DATCorrientePaneles1      

   
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
         #FD2=[]       
         #for i in range(0,len(FD)):
         #    if(FD[i]>(FD[0]/10)):
         #        FD2.append(FD[i])
                 
         #print(f'FD2: {FD2}')
         #print(f'FD largo: {len(FD)}')
         SumMagnitudEficaz = (np.sum([FD[0:len(FD)]]))*0.01
         
         Magnitud1 = FD[0]*0.01
         ArmonicosRestantes=SumMagnitudEficaz-Magnitud1
         #print(f'Irms armonico 1 {q}: {round(Magnitud1,2)}')
         proporcion = irms/(np.sqrt(Magnitud1**2+ArmonicosRestantes**2))
         irmsarmonico1prop=Magnitud1*proporcion
         print(f'Irms armonico 1 proporcionado {q}: {round(irmsarmonico1prop,2)}')
         irmstotalproporcionado=np.sqrt((irmsarmonico1prop**2)+(ArmonicosRestantes*proporcion)**2)
         print(f'Irms total proporcionado{q}: {round(irmstotalproporcionado,2)}')
         #MagnitudArmonicoFundamentalCorriente=round(thd_array[0],3)
         #fp2=round((armonico1corriente*np.cos(phasevoltaje-phasen))/valor1,2)
         #FaseArmonicoFundamentalCorriente=round(np.angle(complejo[0]),2)
         
         #GradoArmonicoFundamentalCorriente=round(Grados,2)
         if(q=="1"):
             global sincvoltaje1
             FDCorrienteCGE1 = irmsarmonico1prop/irms
             print(f'FDCorrienteCGE : {FDCorrienteCGE1 }')
             str_num = {"value":FDCorrienteCGE1,"save":0}
             FDCorrienteCGE = json.dumps(str_num)
             DATCorrienteCGE1 = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             str_num2 = {"value":DATCorrienteCGE1,"save":0}
             DATCorrienteCGE = json.dumps(str_num2)
             #print(f'DAT corriente CGE: {DATCorrienteCGE}')
             phasecorrienteCGE = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje1 == 1):
                 FPCGE0=np.cos(phasevoltajeCGE-phasecorrienteCGE)*FDCorrienteCGE1+0.05
                 cosphiCGE=np.cos(phasevoltajeCGE-phasecorrienteCGE)
                 if(FPCGE0>0.0):
                     FPCGE0=FPCGE0+0.05
                 else:
                     FPCGE0=FPCGE0-0.05
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 print(f'FP1 cge: {FPCGE0}')
                 str_num3 = {"value":FPCGE0,"save":0}
                 FPCGE = json.dumps(str_num3)
                 print(f'cos(phi) cge : {cosphiCGE}')
                 sincvoltaje1=0  
                 #return FPCGE
         #sincvolaje1=0 
         if(q=="3"):
             #print("paso fase 2")
             global sincvoltaje2
             FDCorrientePaneles1 = irmsarmonico1prop/irms
             str_num = {"value":FDCorrientePaneles1,"save":0}
             FDCorrientePaneles = json.dumps(str_num)
             print(f'FDCorrientePaneles : {FDCorrientePaneles1 }')
             DATCorrientePaneles1 = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             str_num2 = {"value":DATCorrientePaneles1,"save":0}
             DATCorrientePaneles = json.dumps(str_num2)
             phasecorrientePaneles = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje2 == 1):
                 FPPaneles1=np.cos(phasevoltajePaneles-phasecorrientePaneles)*FDCorrientePaneles1
                 if (FPPaneles1>0.0):
                     FPPaneles1 = FPPaneles1+0.05
                 else:
                     FPPaneles1 = FPPaneles1-0.05
                 cosphiPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 print(f'FP1 paneles: {FPPaneles1}')
                 print(f'cos(phi) paneles : {cosphiPaneles}')
                 str_num = {"value":FPPaneles1,"save":0}
                 FPPaneles = json.dumps(str_num)
                 sincvoltaje2=0  
                 #return FPCGE
         #sincvoltaje2=0 
         if(q=="2"):
             global sincvoltaje3
             #print("paso fase 3")
             FDCorrienteCarga1=irmsarmonico1prop/irms
             print(f'FD Corriente Carga : {FDCorrienteCarga1}')
             str_num = {"value":FDCorrienteCarga1,"save":0}
             FDCorrienteCarga = json.dumps(str_num)
             DATCorrienteCarga1 = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             str_num2 = {"value":DATCorrienteCarga1,"save":0}
             DATCorrienteCarga = json.dumps(str_num2)
             print(f'DAT carga: {DATCorrienteCarga1}')
             phasecorrienteCarga = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje3 == 1):
                 FPCarga1 = np.cos(phasevoltajeCarga-phasecorrienteCarga)*FDCorrienteCarga1 + 0.05
                 cosphiCarga=np.cos(phasevoltajeCarga-phasecorrienteCarga)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 if(FPCarga1>0.0):
                     FPCarga1=FPCarga1+0.05
                 else:
                     FPCarga1=FPCarga1-0.05
                 print(f'FP carga : {FPCarga1}')
                 print(f'cos(phi) carga : {cosphiCarga}')
                 str_num = {"value":FPCarga1,"save":0}
                 FPCarga = json.dumps(str_num)
                 sincvoltaje3=0





a = datetime.datetime.now()
b = datetime.datetime.now() 
c = datetime.datetime.now()  
#energyCGEFase11 = 0.0
energyCGEFase11 = 0.0
energyPanelesFase12 = 0.0
energyCargaFase13 = 0.0
AparenteCGEFase11 = 0.0
ActivaCGEFase11 = 0.0
ReactivaCGEFase11 = 0.0
AparentePanelesFase12 = 0.0
ActivaPanelesFase12 = 0.0
ReactivaPanelesFase12 = 0.0
AparenteCargaFase13 = 0.0
ActivaCargaFase13 = 0.0
ReactivaCargaFase13 = 0.0
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
          global energyCGEFase1
          global ActivaCGEFase1
          global AparenteCGEFase1
          global ReactivaCGEFase1
          global energyCGEFase11
          global AparenteCGEFase11
          global ActivaCGEFase11
          global ReactivaCGEFase11
          AparenteCGEFase11 = vrms*irms
          print(f'Energia CGE: {AparenteCGEFase11}')
          ActivaCGEFase11 = np.abs(vrms*irms*cosphiCGE)
          print(f'Activa CGE: {ActivaCGEFase11}')
          ReactivaCGEFase11 = vrms*irms*np.sin(phasevoltajeCGE-phasecorrienteCGE)
          print(f'Reactiva CGE: {ReactivaCGEFase11}')
          a2 = datetime.datetime.now()
          delta=(((a2 - a).microseconds)/1000+((a2 - a).seconds)*1000)/10000000000
          energyCGEFase11 += ActivaCGEFase11*delta*2.8
          a = datetime.datetime.now()
          if(a2.hour==0 and a2.minute==0):
              energyCGEFase11=0
          print(f'Energia CGE: {energyCGEFase11}')
          #print(f'Aparente Fase 1: {round(AparenteCGEFase1,2)}')
          str_num = {"value":ActivaCGEFase11,"save":1}
          str_num2 = {"value":ReactivaCGEFase11,"save":0}
          str_num3 = {"value":AparenteCGEFase11,"save":0}
          str_num4 = {"value":energyCGEFase11 ,"save":0}
          ActivaCGEFase1 = json.dumps(str_num)
          AparenteCGEFase1 = json.dumps(str_num3)
          ReactivaCGEFase1 = json.dumps(str_num2)
          energyCGEFase1 = json.dumps(str_num4)
    if(i=="3"):
          global b
          global energyPanelesFase1
          global AparentePanelesFase1
          global ActivaPanelesFase1
          global ReactivaPanelesFase1
          global energyPanelesFase12
          global AparentePanelesFase12
          global ActivaPanelesFase12
          global ReactivaPanelesFase12
          AparentePanelesFase12 = vrms*irms
          print(f'Aparente Paneles: {AparentePanelesFase12}')
          ActivaPanelesFase12= np.abs(vrms*irms*cosphiPaneles)
          print(f'Activa Paneles: {ActivaPanelesFase12}')
          ReactivaPanelesFase12 = vrms*irms*np.sin(phasevoltajePaneles-phasecorrientePaneles)
          b2 = datetime.datetime.now()
          print(f'Reactiva Paneles: {ReactivaPanelesFase12}')
          delta=(((b2 - b).microseconds)/1000+((b2 - b).seconds)*1000)/10000000000
          energyPanelesFase12 += ActivaPanelesFase12*delta#*2.8
          b = datetime.datetime.now()
          if(b2.hour==0 and b2.minute==0):
              energyPanelesFase12=0
          str_num = {"value":ActivaPanelesFase12,"save":1}
          str_num2 = {"value":ReactivaPanelesFase12,"save":0}
          str_num3 = {"value":AparentePanelesFase12,"save":0}
          str_num4 = {"value":energyPanelesFase12,"save":0}
          ActivaPanelesFase1 = json.dumps(str_num)
          AparentePanelesFase1 = json.dumps(str_num3)
          ReactivaPanelesFase1 = json.dumps(str_num2)
          energyPanelesFase1 = json.dumps(str_num4)
    if(i=="2"):
          global c
          global energyCargaFase1 
          global AparenteCargaFase1
          global ActivaCargaFase1
          global ReactivaCargaFase1
          global energyCargaFase13
          global AparenteCargaFase13
          global ActivaCargaFase13
          global ReactivaCargaFase13
          AparenteCargaFase13 = vrms*irms
          print(f'Aparente Carga: {AparenteCargaFase13}')
          ActivaCargaFase13= np.abs(vrms*irms*cosphiCarga)
          print(f'Activa Carga: {ActivaCargaFase13}')
          ReactivaCargaFase13 = vrms*irms*np.sin(phasevoltajeCarga-phasecorrienteCarga)
          c2 = datetime.datetime.now()
          print(f'Reactiva Carga: {ReactivaCargaFase13}')
          delta=(((c2 - c).microseconds)/1000+((c2 - c).seconds)*1000)/10000000000
          energyCargaFase13 += ActivaCargaFase13*delta*2.8
          c = datetime.datetime.now()
          if(c2.hour==0 and c2.minute==0):
              energyCGEFase13=0
          str_num = {"value":ActivaCargaFase13,"save":1}
          str_num2 = {"value":ReactivaCargaFase13,"save":0}
          str_num3 = {"value":AparenteCargaFase13,"save":0}
          str_num4 = {"value":energyCargaFase13,"save":0}
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
vrms11=0.0
vrms22=0.0
vrms33=0.0
irms11=0.0
irms22=0.0
irms33=0.0
modamaximovoltaje11=[]
modamaximocorriente11=[]
modamaximovoltaje22=[]
modamaximocorriente22=[]
modamaximovoltaje33=[]
modamaximocorriente33=[]

def received():
    while True:
                 try:
                     esp32_bytes = esp32.readline()
                     decoded_bytes = str(esp32_bytes[0:len(esp32_bytes)-2].decode("utf-8"))#utf-8
                 except:
                     print("Error en la codificación")
                     continue
                  
                 np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')
                   #print(f'largo array inicial: {len(np_array)}')
        #try:       #        
                 if (len(np_array) == 8402):
                       if (np_array[0] == 11):
                           global modamaximovoltaje11
                           global modamaximocorriente11
                           global vrms1
                           global vrms11
                           global irms1
                           global irms11
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
                           #maximovoltaje2sinmedia=getMaxValues(NoVoltageoffset, 50)
                           #minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 50)
                           #maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                           vrms1=VoltajeRms(NoVoltageoffset)
                          
                           if (len(modamaximovoltaje11)>=10):
                               modavoltaje=np.median(modamaximovoltaje11)
                               vrms1=VoltRms(modavoltaje)
                               print(f'Vrms CGE: {vrms1}')
                               str_num = {"value":vrms1,"save":1}
                               vrms11 = json.dumps(str_num)
                               #print(f'maximo voltaje: {maximovoltaje2}')
                               #print(f'minimo voltaje: {minimovoltaje2}')
                               #NoVoltageoffset2=NoVoltageoffset/1.90
                               #VoltajeRms(NoVoltageoffset2)
                               VoltageFFT(NoVoltageoffset,samplings,1)
                               #graphVoltage1(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                               #graphFFTV1(NoVoltageoffset2,samplings)
                               #print(f'MODA VOLTAJE: {modavoltaje}')
                               
                               modamaximovoltaje11=[]
                           else:
                               modamaximovoltaje11.append(vrms1)
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
                           #maximocorriente2sinmedia=getMaxValues(NoCurrentoffset, 50)
                           #minimocorriente2sinmedia=getMinValues(NoCurrentoffset, 50)
                           #maximocorriente2 = np.median(maximocorriente2sinmedia)
                           #minimocorriente2 = np.median(minimocorriente2sinmedia)
                           irms1=CorrienteRms(NoCurrentoffset)
                           

                           if (len(modamaximocorriente11)>=10):
                               modacorriente=np.median(modamaximocorriente11)
                               irms1=CurrentRms(modacorriente)
                               print(f'Irms CGE: {irms1}')
                               str_num = {"value":irms1,"save":1}
                               irms11 = json.dumps(str_num)
                               #print(f'corriente max: {maximocorriente2 }')
                               #print(f'corriente min: {minimocorriente2 }')
                               #NoCurrentoffset2 = NoCurrentoffset/125  #210 con res
                               #irms1 = CorrienteRms(NoCurrentoffset2)
                               CurrentFFT(NoCurrentoffset,samplings,1,irms1)
                               print(f'MODA CORRIENTE CGE: {modacorriente}')
                               Potencias(1,irms1,vrms1)
                               modamaximocorriente11=[]
                           else:
                               modamaximocorriente11.append(irms1)
                           #    print(f'array corriente: {modamaximocorriente2}')

                       if (np_array[0] == 22):
                           #global modamaximovoltaje2
                           #global modamaximocorriente2
                           #print("22")
                           global modamaximovoltaje22
                           global modamaximocorriente22
                           global vrms2
                           global vrms22
                           global irms2
                           global irms22
                           global modavoltaje22
                           global modacorriente22
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
                           #maximovoltaje2sinmedia=getMaxValues(NoVoltageoffset, 50)
                           #minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 50)
                           #maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                           vrms2=VoltajeRms(NoVoltageoffset)
                           
                           if (len(modamaximovoltaje22)>=10):
                               modavoltaje22=np.median(modamaximovoltaje22)
                               vrms2=VoltRms(modavoltaje22)
                               str_num = {"value":vrms2,"save":1}
                               vrms22 = json.dumps(str_num)
                               print(f'Vrms Carga: {vrms2}')
                               #print(f'minimo voltaje: {minimovoltaje2}')
                               #NoVoltageoffset2=NoVoltageoffset/1.90
                               #VoltajeRms(NoVoltageoffset2)
                               #graphVoltage1(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                               #graphFFTV1(NoVoltageoffset2,samplings)
                               #print(f'MODA VOLTAJE: {modavoltaje}')
                               VoltageFFT(NoVoltageoffset,samplings,2)
                               modamaximovoltaje22=[]
                           else:
                               modamaximovoltaje22.append(vrms2)
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
                           #maximocorriente2sinmedia=getMaxValues(NoCurrentoffset, 50)
                           #minimocorriente2sinmedia=getMinValues(NoCurrentoffset, 50)
                           #maximocorriente2 = np.median(maximocorriente2sinmedia)
                           #minimocorriente2 = np.median(minimocorriente2sinmedia)
                           irms2=CorrienteRms(NoCurrentoffset)

                           if (len(modamaximocorriente22)>=10):
                               modacorriente22=np.median(modamaximocorriente22)
                               irms2=CurrentRms(modacorriente22)
                               str_num = {"value":irms2,"save":1}
                               irms22 = json.dumps(str_num)
                               print(f'Irms Carga: {irms2}')
                               print(f'MODA CORRIENTE Carga: {modacorriente22}')
                               CurrentFFT(NoCurrentoffset,samplings,2,irms2)
                               Potencias(2,irms2,vrms2)
                               modamaximocorriente22=[]
                           else:
                               modamaximocorriente22.append(irms2)

                       if (np_array[0] == 33):
                           global modamaximovoltaje33
                           global modamaximocorriente33
                           #print("33")
                           global vrms3
                           global vrms33
                           global irms3
                           global irms33
                           global modavoltaje33
                           global modacorriente33
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
                           #maximovoltaje2sinmedia=getMaxValues(NoVoltageoffset, 50)
                           #minimovoltaje2sinmedia=getMinValues(NoVoltageoffset, 50)
                           #maximovoltaje2 = np.median(maximovoltaje2sinmedia)
                           #vrms1=VoltRms(maximovoltaje2)
                           #minimovoltaje2 = np.median(minimovoltaje2sinmedia)
                           
                           vrms3=VoltajeRms(NoVoltageoffset)
                           
                           if (len(modamaximovoltaje33)>=10):
                               modavoltaje33=np.median(modamaximovoltaje33)
                               vrms3=VoltRms(modavoltaje33)
                               str_num = {"value":vrms3,"save":1}
                               vrms33 = json.dumps(str_num)
                               print(f'Vrms Paneles: {vrms3}')
                               #print(f'minimo voltaje: {minimovoltaje2}')
                               #NoVoltageoffset2=NoVoltageoffset/1.90
                               #VoltajeRms(NoVoltageoffset2)
                               #graphVoltage1(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                               #graphFFTV1(NoVoltageoffset2,samplings)
                               #print(f'MODA VOLTAJE: {modavoltaje}')
                               VoltageFFT(NoVoltageoffset,samplings,3)
                               modamaximovoltaje33=[]
                           else:
                               modamaximovoltaje33.append(vrms3)
                               #print(f'array voltaje: {modamaximovoltaje2}')
                            
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
                           irms3=CorrienteRms(NoCurrentoffset)
                           

                           if (len(modamaximocorriente33)>=10):
                               modacorriente33=np.median(modamaximocorriente33)
                               irms3=CurrentRms(modacorriente33)
                               str_num = {"value":irms3,"save":1}
                               irms33 = json.dumps(str_num)
                               print(f'Irms Paneles : {irms3}')
                               print(f'MODA CORRIENTE Paneles: {modacorriente33}')
                               CurrentFFT(NoCurrentoffset,samplings,3,irms3)
                               Potencias(3,irms3,vrms3)
                               modamaximocorriente33=[]
                           else:
                               modamaximocorriente33.append(irms3)
                   
                 if (len(np_array)>0 and len(np_array)<=2):
                         global tempESP32
                         global Temp_Raspberry
                         #global EstateVentilador
                         Temp_Raspberry0=cpu_temp()
                         print("Temperatura Raspberry:  ",Temp_Raspberry0)
                         str_num = {"value":Temp_Raspberry0,"save":1}
                         Temp_Raspberry = json.dumps(str_num)
                         Ventilador()
                         #temphum()
                         #distance()
                         tempESP320 = round(np_array[0],0)
                         str_num2 = {"value":tempESP320,"save":1}
                         tempESP32 = json.dumps(str_num2)
                         #print(f'array: {np_array}')
                 Maximo15minCGE()
                 Maximo15minCarga()
                 Maximo15minPaneles()
                 excel=datetime.datetime.now()
                 if(excel.hour==13 or excel.minute==00):
                     SendEmail()
                 if(excel.minute==1 or excel.minute==16 or excel.minute==31 or excel.minute==46):
                     ExcelDataCGE()
                     ExcelDataCarga()
                     ExcelDataPaneles()

                 try:  
                       if(client.connected_flag==True): 
                             #print("paso")
                             publish(client)
                 except:
                     #print("rc: 0")
                     continue
        #except:
        #    print("Error en Bucle")
        #    continue


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
        global v12, v13, v14, v15, v16, v17, w1, x1, y1, z1
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
                     result = client.publish(topic1, irms11)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send irms1: `{irms11}` to topic `{topic1}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic1}")
        
                   
            if(i["variableFullName"]=="Voltaje-CGE"):
                freq = i["variableSendFreq"]
                if(a1 - c1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     c1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, vrms11)
                     status = result[0]
                     if status == 0:
                         print(f"Send vrms1: `{vrms11}` to topic `{topic2}` con freq: {freq}")
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
                         print(f"Send Pot-Reactiva-CGE: `{ReactivaCGEFase1}` to topic `{topic3}` con freq: {freq}")
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
                         print(f"Send Pot-Activa CGE: `{ActivaCGEFase1}` to topic `{topic4}` con freq: {freq}")
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
                         print(f"Send energia CGE: `{energyCGEFase1}` to topic `{topic5}` con freq: {freq}")
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
                         print(f"Send FP-CGE: `{FPCGE}` to topic `{topic5}` con freq: {freq}")
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
                         print(f"Send FD-CGE: `{FDCorrienteCGE}` to topic `{topic5}` con freq: {freq}")
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
                         print(f"Send Pot-Aparente-CGE : `{AparenteCGEFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")

            #SEGUNDA TOMA
            if(i["variableFullName"]=="Corriente-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - l1 > float(freq)):
                     l1=time.time()
                     str_variable = i["variable"]
                     topic = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, irms22)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send Corriente-Carga: `{irms22}` to topic `{topic}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic}")
        
                   
            if(i["variableFullName"]=="Voltaje-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - m1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     m1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, vrms22)
                     status = result[0]
                     if status == 0:
                         print(f"Send Voltaje-Carga: `{vrms22}` to topic `{topic2}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic2}")
            
            if(i["variableFullName"]=="Potencia-Reactiva-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - n1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     n1=time.time()
                     str_variable3 = i["variable"]
                     topic3 = topicmqtt + str_variable3 + "/sdata"
                     result = client.publish(topic3, ReactivaCargaFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Potencia-Reactiva-Carga: `{ReactivaCargaFase1}` to topic `{topic3}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic3}")
            
            if(i["variableFullName"]=="Pot-Activa-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - o21 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     o21=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, ActivaCargaFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Pot-Activa-Carga: `{ActivaCargaFase1}` to topic `{topic4}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic4}")
            
            if(i["variableFullName"]=="Energia-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - p1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     p1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energyCargaFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Energia-Carga: `{energyCargaFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="FP-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - q1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     q1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FPCarga)
                     status = result[0]
                     if status == 0:
                         print(f"Send FP-Carga: `{FPCarga}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="FD-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - r1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     r1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FDCorrienteCarga)
                     status = result[0]
                     if status == 0:
                         print(f"Send FD-Carga: `{FDCorrienteCarga}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="DAT-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - s1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     s1=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, DATCorrienteCarga)
                     status = result[0]
                     if status == 0:
                         print(f"Send DAT-Carga: `{DATCorrienteCarga}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="Pot-Aparente-Carga"):
                freq = i["variableSendFreq"]
                if(a1 - t1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     t1=time.time()
                     str_variable = i["variable"]
                     topic5= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, AparenteCargaFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Pot-Aparente-Carga: `{AparenteCargaFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            
            #Tercera Toma
            if(i["variableFullName"]=="Corriente-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - u1 > float(freq)):
                     u1=time.time()
                     str_variable = i["variable"]
                     topic = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, irms33)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send Corriente-Paneles: `{irms33}` to topic `{topic}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic}")
        
                   
            if(i["variableFullName"]=="Voltaje-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v1=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, vrms33)
                     status = result[0]
                     if status == 0:
                         print(f"Send Voltaje-Paneles: `{vrms33}` to topic `{topic2}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic2}")
            
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
            
            if(i["variableFullName"]=="Pot-Activa-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v13 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v13=time.time()
                     str_variable4 = i["variable"]
                     topic4 = topicmqtt + str_variable4 + "/sdata"
                     result = client.publish(topic4, ActivaPanelesFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Pot-Activa-Paneles: `{ActivaPanelesFase1}` to topic `{topic4}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic4}")
            
            if(i["variableFullName"]=="Energia-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v14 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v14=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, energyPanelesFase1)
                     status = result[0]
                     if status == 0:
                         print(f"Send Energia-Paneles: `{energyPanelesFase1}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
            if(i["variableFullName"]=="FP-Paneles"):
                freq = i["variableSendFreq"]
                if(a1 - v15 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     v15=time.time()
                     str_variable = i["variable"]
                     topic5 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic5, FPPaneles)
                     status = result[0]
                     if status == 0:
                         print(f"Send FP-Paneles: `{FPPaneles}` to topic `{topic5}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic5}")
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
            
            
            if(i["variableFullName"]=="Temperatura-ESP32"):
                freq = i["variableSendFreq"]
                if(a1 - x1 > float(freq)):
                     #print("varlastsend 1: ",varsLastSend[i])
                     x1=time.time()
                     str_variable = i["variable"]
                     topic= topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic, tempESP32)
                     status = result[0]
                     if status == 0:
                         print(f"Send Temperatura-ESP32: `{tempESP32}` to topic `{topic}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic}")

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
            
"""
'Voltaje', 'Corriente','Potencia Activa','Potencia Reactiva','Potencia Aparente',
'FP','FD','DAT',
"""
Volt15CGE=[]
dataCGE=[]
acceso = 0
maximoVoltaje15CGE=0
maximoCorrienteCGE=0
maximoPotActivaCGE=0
maximoPotReactivaCGE=0
maximoPotAparenteCGE=0
maximoFPCGE=0
maximoFDCGE=0
maximoDATCGE=0
Corriente15CGE=[]
PotActiva15CGE=[]
PotReactiva15CGE=[]
PotAparente15CGE=[]
FP15CGE=[]
FD15CGE=[]
DAT15CGE=[]
def Maximo15minCGE():
    global maximoVoltaje15CGE
    global maximoCorrienteCGE
    global maximoPotActivaCGE
    global maximoPotReactivaCGE
    global maximoPotAparenteCGE
    global maximoFPCGE
    global maximoFDCGE
    global maximoDATCGE
    global Corriente15CGE
    global PotActiva15CGE
    global PotReactiva15CGE
    global PotAparente15CGE
    global FP15CGE
    global FD15CGE
    global DAT15CGE
    global Volt15CGE
    global acceso
    basea = datetime.datetime.now()
    print(f'Maximo Voltaje 15 CGE: {maximoVoltaje15CGE}')
    if(basea.minute==0 or basea.minute==15 or basea.minute==30 or basea.minute==45): 
         print("paso if")
         if(acceso == 0):
              print("paso if 2")
              acceso = 1
              maximoVoltaje15CGE=max(Volt15CGE)
              maximoCorrienteCGE=max(Corriente15CGE)
              maximoPotActivaCGE=max(PotActiva15CGE)
              maximoPotReactivaCGE=max(PotReactiva15CGE)
              maximoPotAparenteCGE=max(PotAparente15CGE)
              maximoFPCGE=min(FP15CGE)
              maximoFDCGE=max(FD15CGE)
              maximoDATCGE=max(DAT15CGE)
              dataCGE.insert(1,maximoVoltaje15CGE)
              dataCGE.insert(2,maximoCorrienteCGE)
              dataCGE.insert(3,maximoPotActivaCGE)
              dataCGE.insert(4,maximoPotReactivaCGE)
              dataCGE.insert(5,maximoPotAparenteCGE)
              dataCGE.insert(6,maximoFPCGE)
              dataCGE.insert(7,maximoFDCGE)
              dataCGE.insert(8,maximoDATCGE)
              Volt15CGE=[]
              Corriente15CGE=[]
              PotActiva15CGE=[]
              PotReactiva15CGE=[]
              PotAparente15CGE=[]
              FP15CGE=[]
              FD15CGE=[]
              DAT15CGE=[]
         elif(acceso==1):
              print("paso elif CGE")
              Volt15CGE.append(vrms1)
              Corriente15CGE.append(irms1)
              PotActiva15CGE.append(ActivaCGEFase11)
              PotReactiva15CGE.append(ReactivaCGEFase11)
              PotAparente15CGE.append(AparenteCGEFase11)
              FP15CGE.append(FPCGE0)
              FD15CGE.append(FDCorrienteCGE1)
              DAT15CGE.append(DATCorrienteCGE1)
 
    else:
        Volt15CGE.append(vrms1)
        Corriente15CGE.append(irms1)
        PotActiva15CGE.append(ActivaCGEFase11)
        PotReactiva15CGE.append(ReactivaCGEFase11)
        PotAparente15CGE.append(AparenteCGEFase11)
        FP15CGE.append(FPCGE0)
        FD15CGE.append(FDCorrienteCGE1)
        DAT15CGE.append(DATCorrienteCGE1)
        acceso = 0
        if(len(Volt15CGE)>4):
            indice=np.argmin(Volt15CGE)
            Volt15CGE.pop(indice)
            print(f'Volt15CGE Despúes: {Volt15CGE}')
            indice=np.argmin(Corriente15CGE)
            Corriente15CGE.pop(indice)
            indice=np.argmin(PotActiva15CGE)
            PotActiva15CGE.pop(indice)
            indice=np.argmin(PotReactiva15CGE)
            PotReactiva15CGE.pop(indice)
            indice=np.argmin(PotAparente15CGE)
            PotAparente15CGE.pop(indice)
            indice=np.argmax(FP15CGE)
            FP15CGE.pop(indice)
            indice=np.argmin(FD15CGE)
            FD15CGE.pop(indice)
            indice=np.argmin(DAT15CGE)
            DAT15CGE.pop(indice)
             

Volt15Carga=[]
dataCarga=[]
accesoCarga = 0
maximoVoltaje15Carga=0
maximoCorrienteCarga=0
maximoPotActivaCarga=0
maximoPotReactivaCarga=0
maximoPotAparenteCarga=0
maximoFPCarga=0
maximoFDCarga=0
maximoDATCarga=0
Corriente15Carga=[]
PotActiva15Carga=[]
PotReactiva15Carga=[]
PotAparente15Carga=[]
FP15Carga=[]
FD15Carga=[]
DAT15Carga=[]
def Maximo15minCarga():
    global maximoVoltaje15Carga
    global maximoCorrienteCarga
    global maximoPotActivaCarga
    global maximoPotReactivaCarga
    global maximoPotAparenteCarga
    global maximoFPCarga
    global maximoFDCarga
    global maximoDATCarga
    global Corriente15Carga
    global PotActiva15Carga
    global PotReactiva15Carga
    global PotAparente15Carga
    global FP15Carga
    global FD15Carga
    global DAT15Carga
    global Volt15Carga
    global accesoCarga
    basea = datetime.datetime.now()
    print(f'Maximo Voltaje 15 Carga: {maximoVoltaje15Carga}')
    if(basea.minute==0 or basea.minute==15 or basea.minute==30 or basea.minute==45): 
         print("paso if Carga")
         if(accesoCarga == 0):
              print("paso if 2 Carga")
              accesoCarga = 1
              maximoVoltaje15Carga=max(Volt15Carga)
              maximoCorrienteCarga=max(Corriente15Carga)
              maximoPotActivaCarga=max(PotActiva15Carga)
              maximoPotReactivaCarga=max(PotReactiva15Carga)
              maximoPotAparenteCarga=max(PotAparente15Carga)
              maximoFPCarga=min(FP15Carga)
              maximoFDCarga=max(FD15Carga)
              maximoDATCarga=max(DAT15Carga)
              dataCarga.insert(1,maximoVoltaje15Carga)
              dataCarga.insert(2,maximoCorrienteCarga)
              dataCarga.insert(3,maximoPotActivaCarga)
              dataCarga.insert(4,maximoPotReactivaCarga)
              dataCarga.insert(5,maximoPotAparenteCarga)
              dataCarga.insert(6,maximoFPCarga)
              dataCarga.insert(7,maximoFDCarga)
              dataCarga.insert(8,maximoDATCarga)
              Volt15Carga=[]
              Corriente15Carga=[]
              PotActiva15Carga=[]
              PotReactiva15Carga=[]
              PotAparente15Carga=[]
              FP15Carga=[]
              FD15Carga=[]
              DAT15Carga=[]
         elif(accesoCarga==1):
              print("paso elif Carga")
              Volt15Carga.append(vrms1)
              Corriente15Carga.append(irms2)
              PotActiva15Carga.append(ActivaCargaFase13)
              PotReactiva15Carga.append(ReactivaCargaFase13)
              PotAparente15Carga.append(AparenteCargaFase13)
              FP15Carga.append(FPCarga1)
              FD15Carga.append(FDCorrienteCarga1)
              DAT15Carga.append(DATCorrienteCarga1)
              
    else:
        Volt15Carga.append(vrms2)
        Corriente15Carga.append(irms2)
        PotActiva15Carga.append(ActivaCargaFase13)
        PotReactiva15Carga.append(ReactivaCargaFase13)
        PotAparente15Carga.append(AparenteCargaFase13)
        FP15Carga.append(FPCarga1)
        FD15Carga.append(FDCorrienteCarga1)
        DAT15Carga.append(DATCorrienteCarga1)
        accesoCarga = 0
        if(len(Volt15Carga)>4):
            indice=np.argmin(Volt15Carga)
            Volt15Carga.pop(indice)
            print(f'Volt15Carga Despúes: {Volt15Carga}')
            indice=np.argmin(Corriente15Carga)
            Corriente15Carga.pop(indice)
            indice=np.argmin(PotActiva15Carga)
            PotActiva15Carga.pop(indice)
            indice=np.argmin(PotReactiva15Carga)
            PotReactiva15Carga.pop(indice)
            indice=np.argmin(PotAparente15Carga)
            PotAparente15Carga.pop(indice)
            indice=np.argmax(FP15Carga)
            FP15Carga.pop(indice)
            indice=np.argmin(FD15Carga)
            FD15Carga.pop(indice)
            indice=np.argmin(DAT15Carga)
            DAT15Carga.pop(indice)


Volt15Paneles=[]
dataPaneles=[]
accesoPaneles = 0
maximoVoltaje15Paneles=0
maximoCorrientePaneles=0
maximoPotActivaPaneles=0
maximoPotReactivaPaneles=0
maximoPotAparentePaneles=0
maximoFPPaneles=0
maximoFDPaneles=0
maximoDATPaneles=0
Corriente15Paneles=[]
PotActiva15Paneles=[]
PotReactiva15Paneles=[]
PotAparente15Paneles=[]
FP15Paneles=[]
FD15Paneles=[]
DAT15Paneles=[]
def Maximo15minPaneles():
    global maximoVoltaje15Paneles
    global maximoCorrientePaneles
    global maximoPotActivaPaneles
    global maximoPotReactivaPaneles
    global maximoPotAparentePaneles
    global maximoFPPaneles
    global maximoFDPaneles
    global maximoDATPaneles
    global Corriente15Paneles
    global PotActiva15Paneles
    global PotReactiva15Paneles
    global PotAparente15Paneles
    global FP15Paneles
    global FD15Paneles
    global DAT15Paneles
    global Volt15Paneles
    global accesoPaneles
    basea = datetime.datetime.now()
    print(f'Maximo Voltaje 15 Paneles: {maximoVoltaje15Paneles}')
    if(basea.minute==0 or basea.minute==15 or basea.minute==30 or basea.minute==45): 
         print("paso if Paneles")
         if(accesoPaneles == 0):
              print("paso if 2 Paneles")
              accesoPaneles = 1
              maximoVoltaje15Paneles=max(Volt15Paneles)
              maximoCorrientePaneles=max(Corriente15Paneles)
              maximoPotActivaPaneles=max(PotActiva15Paneles)
              maximoPotReactivaPaneles=max(PotReactiva15Paneles)
              maximoPotAparentePaneles=max(PotAparente15Paneles)
              maximoFPPaneles=min(FP15Paneles)
              maximoFDPaneles=max(FD15Paneles)
              maximoDATPaneles=max(DAT15Paneles)
              dataPaneles.insert(1,maximoVoltaje15Paneles)
              dataPaneles.insert(2,maximoCorrientePaneles)
              dataPaneles.insert(3,maximoPotActivaPaneles)
              dataPaneles.insert(4,maximoPotReactivaPaneles)
              dataPaneles.insert(5,maximoPotAparentePaneles)
              dataPaneles.insert(6,maximoFPPaneles)
              dataPaneles.insert(7,maximoFDPaneles)
              dataPaneles.insert(8,maximoDATPaneles)
              Volt15Paneles=[]
              Corriente15Paneles=[]
              PotActiva15Paneles=[]
              PotReactiva15Paneles=[]
              PotAparente15Paneles=[]
              FP15Paneles=[]
              FD15Paneles=[]
              DAT15Paneles=[]
         elif(accesoPaneles==1):
              print("paso elif Paneles")
              Volt15Paneles.append(vrms3)
              Corriente15Paneles.append(irms3)
              PotActiva15Paneles.append(ActivaPanelesFase12)
              PotReactiva15Paneles.append(ReactivaPanelesFase12)
              PotAparente15Paneles.append(AparentePanelesFase12)
              FP15Paneles.append(FPCarga1)
              FD15Paneles.append(FDCorrientePaneles1)
              DAT15Paneles.append(DATCorrientePaneles1)
              
              
 
    else:
        Volt15Paneles.append(vrms3)
        Corriente15Paneles.append(irms3)
        PotActiva15Paneles.append(ActivaPanelesFase12)
        PotReactiva15Paneles.append(ReactivaPanelesFase12)
        PotAparente15Paneles.append(AparentePanelesFase12)
        FP15Paneles.append(FPPaneles1)
        FD15Paneles.append(FDCorrientePaneles1)
        DAT15Paneles.append(DATCorrientePaneles1)      
        accesoPaneles = 0
        if(len(Volt15Carga)>4):
            indice=np.argmin(Volt15Paneles)
            Volt15Paneles.pop(indice)
            print(f'Volt15Carga Despúes: {Volt15Paneles}')
            indice=np.argmin(Corriente15Paneles)
            Corriente15Paneles.pop(indice)
            indice=np.argmin(PotActiva15Paneles)
            PotActiva15Paneles.pop(indice)
            indice=np.argmin(PotReactiva15Paneles)
            PotReactiva15Paneles.pop(indice)
            indice=np.argmin(PotAparente15Paneles)
            PotAparente15Paneles.pop(indice)
            indice=np.argmax(FP15Paneles)
            FP15Paneles.pop(indice)
            indice=np.argmin(FD15Paneles)
            FD15Paneles.pop(indice)
            indice=np.argmin(DAT15Paneles)
            DAT15Paneles.pop(indice)


book = Workbook()
dest_filename = 'Reportes_csv.xlsx'
sheet = book.active
sheet.title = "Resumen Reportes"
sheet2 = book.create_sheet("CGE")
sheet3 = book.create_sheet("Carga")
sheet4 = book.create_sheet("Paneles")


headings=['Fecha y Hora'] + list(['Voltaje', 'Corriente','Potencia Activa','Potencia Reactiva','Potencia Aparente',
'FP','FD','DAT','Energia'])
sheet2.append(headings)
sheet3.append(headings)
sheet4.append(headings)
book.save(filename = dest_filename)


accesoexcel=0
def ExcelDataCGE():
       global dataCGE
       global accesoexcel
       base=datetime.datetime.now()
       if(base.minute==1 or base.minute==16 or base.minute==31 or base.minute==46):
               if(accesoexcel==0):              
                       workbook=openpyxl.load_workbook(filename = dest_filename)
                       sheet2 = workbook["CGE"]
                       dataCGE.insert(0,datetime.datetime.now())
                       sheet2.append(list(dataCGE))
                       print(f'Data Carga: {dataCGE}')
                       print("Datos Insertados Correctamente!")
                       workbook.save(filename = dest_filename)
                       dataCGE=[]
                       accesoexcel=1
       else:
               accesoexcel=0


AccesoExcelCarga=0
def ExcelDataCarga():
       global dataCarga
       global AccesoExcelCarga
       base=datetime.datetime.now()
       if(base.minute==1 or base.minute==16 or base.minute==31 or base.minute==46):
               if(AccesoExcelCarga==0):              
                       workbook=openpyxl.load_workbook(filename = dest_filename)
                       sheet2 = workbook["CGE"]
                       dataCarga.insert(0,datetime.datetime.now())
                       sheet2.append(list(dataCGE))
                       print(f'Data Carga: {dataCarga}')
                       print("Datos Insertados Correctamente!")
                       workbook.save(filename = dest_filename)
                       dataCarga=[]
                       AccesoExcelCarga=1
       else:
               AccesoExcelCarga=0

AccesoExcelPaneles=0
def ExcelDataPaneles():
       global dataPaneles
       global AccesoExcelPaneles
       base=datetime.datetime.now()
       if(base.minute==1 or base.minute==16 or base.minute==31 or base.minute==46):
               if(AccesoExcelPaneles==0):              
                       workbook=openpyxl.load_workbook(filename = dest_filename)
                       sheet2 = workbook["CGE"]
                       dataPaneles.insert(0,datetime.datetime.now())
                       sheet2.append(list(dataPaneles))
                       print(f'Data Carga: {dataPaneles}')
                       print("Datos Insertados Correctamente!")
                       workbook.save(filename = dest_filename)
                       dataPaneles=[]
                       AccesoExcelPaneles=1
       else:
               AccesoExcelPaneles=0

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

