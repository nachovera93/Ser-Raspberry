#import ast
#import csv
import threading
#from PIL import ImageTk, Image
#import pytz
#from datetime import datetime
import datetime
#import matplotlib
from scipy import interpolate
from scipy.fft import fft, fftfreq
from scipy.interpolate import lagrange
from scipy import signal
from scipy.signal import savgol_filter
#import pandas as pd
import numpy as np
from flask import Flask,render_template, redirect, request
import subprocess
from time import sleep
#import psutil
import sys
import socket
import RPi.GPIO as GPIO
import time
#from PIL import Image
#from PIL import ImageTk
import glob
import os
import serial
#from matplotlib.backends.backend_tkagg import (
   # FigureCanvasTkAgg, NavigationToolbar2Tk)
#import matplotlib.pyplot as plt
#from matplotlib.figure import Figure
#plt.style.use('ggplot')
#from flask_moment import Moment
import math
import board
#import adafruit_dht

#dhtDevice = adafruit_dht.DHT11(board.D18)

app = Flask(__name__)

#global list_Current1
#global list_Current2
#global list_Current3
#global list_Voltage1
#global list_temp

#moment = Moment(app)
"""

if not os.path.exists('log/'):
    os.makedirs('log/')

if not os.path.exists('images/'):
    os.makedirs('images/')

if not os.path.exists('images/fft'):
    os.makedirs('images/fft')

if not os.path.exists('images/fft/voltage'):
    os.makedirs('images/fft/voltage')

if not os.path.exists('images/fft/current1'):
    os.makedirs('images/fft/current1')

if not os.path.exists('images/fft/current2'):
    os.makedirs('images/fft/current2')

if not os.path.exists('images/fft/current3'):
    os.makedirs('images/fft/current3')

if not os.path.exists('images/señal'):
    os.makedirs('images/señal')

if not os.path.exists('images/señal/current1'):
    os.makedirs('images/señal/current1')

if not os.path.exists('images/señal/current2'):
    os.makedirs('images/señal/current2')

if not os.path.exists('images/señal/current3'):
    os.makedirs('images/señal/current3')

if not os.path.exists('images/señal/voltage'):
    os.makedirs('images/señal/voltage')

#font = {'family': 'serif',
 #       'color':  'darkred',
  #      'weight': 'normal',
   #     'size': 16,
    #    }

"""

esp32 = serial.Serial('/dev/ttyUSB0', 250000, timeout=0.5)
esp32.flushInput()

#TRIG = 4
#ECHO = 24

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


pins = {
   8 : {'name' : 'GPIO 8', 'state' : GPIO.LOW}
   }

for pin in pins:
   GPIO.setup(pin, GPIO.OUT)
   GPIO.output(pin, GPIO.LOW)

def get_ip_address():
    global ip_address
    ip_address = ''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_address = s.getsockname()[0]
    s.close()
    #return ip_address


get_ip_address()


def cpu_temp():
	thermal_zone = subprocess.Popen(
	    ['cat', '/sys/class/thermal/thermal_zone0/temp'], stdout=subprocess.PIPE)
	out, err = thermal_zone.communicate()
	cpu_temp = int(out.decode())/1000
	return(cpu_temp)


def get_cpuload():
    cpuload = psutil.cpu_percent(interval=1, percpu=False)
    return str(cpuload)


CPU_temp = 0.0
def getTEMP():
    global CPU_temp
    CPU_temp = round(cpu_temp(),0)
    #print(f'temp cpu: {CPU_temp}')
    if CPU_temp > 50:
        #print("Ventilador on")
        GPIO.output(23, True)
    elif CPU_temp <= 40:
        #print("Ventilador off")
        GPIO.output(23, False)



temperatura = 0.0
humedad = 0.0
"""
def temphum():
    global temperatura
    global humedad
    try:
        temperatura = dhtDevice.temperature
        #temperature_f = temperature_c * (9 / 5) + 32
        humedad = dhtDevice.humidity
        #print("Temp: {:.1f} C    Humidity: {}% ".format( temperatura, humedad))
        #return humidity,temperature
    except RuntimeError as error:
        # Errors happen fairly often, DHT's are hard to read, just keep going
        print(error.args[0])
        #time.sleep(2.0)
        #continue
    #except Exception as error:
     #   dhtDevice.exit()
      #  raise error 
"""


def distance():
    global puerta
    global start
    global end  
    try:
         GPIO.output(4, True)
         time.sleep(0.00001)
         GPIO.output(4, False)
         while GPIO.input(24) == False:
               start = time.time()
         while GPIO.input(24) == True:
               end = time.time()
         sig_time = end-start
         distance = round(sig_time / 0.000058,1)   #cm   
         if(distance > 15):
              puerta = "Abierta"
         elif(distance<15):
              puerta = "Cerrada"
         print('Distance: {} centimeters'.format(distance))
    except RuntimeError as error:
        # Errors happen fairly often, DHT's are hard to read, just keep going
         print(error.args[0])
        
        


# def get_uptime():
 #    with open('/proc/uptime', 'r') as f:
  #   uptime_seconds = float(f.readline().split()[0])
   #  uptime = (timedelta(seconds = uptime_seconds))
     # Label(root, text=uptime,font=('Arial', 16)).grid(row=15, column=1)
    #  return str(uptime)




def getMaxValues(myList, quantity):
        return(sorted(list(set(myList)), reverse=True)[:quantity]) 
        #print(f'max : {max(myList)}')


def getMinValues(myList, quantity):
        return(sorted(list(set(myList)))[:quantity]) 
        #print(f'max : {max(myList)}')

"""
715/z=282Vpk
z=2.53
748/z=309VPk
z=2.42
"""
def EscalaVoltaje(voltaje):
    if(max(voltaje)>=735):
        newvoltaje=voltaje*0.57
    elif(max(voltaje)<735):
        newvoltaje=voltaje*0.545

    return newvoltaje

def EscalaCorriente(corriente):
    if(max(corriente)>=850):
        newcorriente=corriente/54   
    elif(max(corriente)<850):
        newcorriente=corriente/140 

    return newcorriente           

"""
918 = 11.17APeak

"""

"""
428 = 308VPk
392 = 282Vpk
"""
def VoltajeRms(listVoltage):
    #global vrms
    print(f'maximo voltaje 2 : {max(listVoltage)}')
    #listVoltage=listVoltage*0.81
    if(max(listVoltage)>=405):
        listVoltage=listVoltage*0.73
    elif(max(listVoltage)<400):
        listVoltage=listVoltage*0.65
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
    print(f'Voltaje RMS : {vrms}')
    return vrms


def CorrienteRms(listCurrent):
    
    print(f'maximo corriente 2 : {max(listCurrent)}')
    if(max(listCurrent)>1):
        listCurrent=listCurrent*1.2
    elif(max(listCurrent)<1):
        listCurrent=listCurrent/1.5
    
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
    print(f'Corriente RMS : {irms}')
    return irms




def graphVoltageCurrent(listVoltage,listCurrent,samplings): ##Grafica corriente y Voltaje
        global labelsfp
        global valuesvoltage
        global valuescurrent
        global maxvoltaje
        global minvoltaje
        #global maxcorriente
        #global mincorriente    
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]

        #f = interpolate.interp1d(tiempoms, listVoltage)
        #xnew = np.arange(0, 4096, 5)  # 2550
        # print(f'largo xnew : {len(xnew)}')
        #ynew = f(xnew)
        valores = listVoltage#[200:4000]
        valores2 = listCurrent#[200:4000]
        #valores=round(val,1)
        valuesvoltage = [ i for i in valores ]
        valuescurrent = [ i for i in valores2 ]
        #labelsfp = [ i for i in range(0,len(valuesvoltage))]
        labelsfp =  [ i for i in my_formatted_list]
        maxvoltaje = max(valores)+300
        minvoltaje = min(valores)-300
        
        


def graphVoltage1(list_fftVoltage,maximovoltaje,minimovoltaje,samplings): ##Grafica voltaje CGE
        global labelsvoltaje1
        global valuesvoltaje1
        global ejeyV11
        global ejeyV12
        valores = list_fftVoltage#[1000:4000]
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]
        #valores=round(val,1)
        valuesvoltaje1 = [ i for i in valores ]
        labelsvoltaje1 = [ i for i in my_formatted_list ]
        ejeyV11 = maximovoltaje + 100
        ejeyV12 = minimovoltaje - 100
        print(f'values voltaje: {len(labelsvoltaje1)}')
        #Graficar png
           # plt.figure(figsize=(15, 5))
           #plt.plot(list_fftVoltage)
           #oldepoch = time.time()
           #st = datetime.datetime.fromtimestamp(
           #    oldepoch).strftime('%Y-%m-%d-%H:%M:%S')
           #plt.title("Voltaje Fase 1", fontdict=font)
           #plt.ylabel("Voltage (V-Peak-Peak) ", fontdict=font)
           #plt.xlabel("Tiempo(s)", fontdict=font)
           # plt.savefig("images/señal/voltage/"+st+"Voltage1.png")
           # plt.close(fig)




def graphVoltage2(list_fftVoltage,maximovoltaje,minimovoltaje,samplings): ##Grafica voltaje Paneles
        global labelsvoltaje2
        global valuesvoltaje2
        global ejeyV21
        global ejeyV22
        valores = list_fftVoltage#[1000:4000]
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]
        #valores=round(val,1)
        valuesvoltaje2 = [ i for i in valores ]
        labelsvoltaje2 = [ i for i in my_formatted_list ]
        ejeyV21 = maximovoltaje + 100
        ejeyV22 = minimovoltaje - 100



def graphVoltage3(list_fftVoltage,maximovoltaje,minimovoltaje,samplings): ##Grafica voltaje Carga
        global labelsvoltaje3
        global valuesvoltaje3
        global ejeyV31
        global ejeyV32
        valores = list_fftVoltage#[1000:4000]
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]
        #valores=round(val,1)
        valuesvoltaje3 = [ i for i in valores ]
        labelsvoltaje3 = [ i for i in my_formatted_list ]
        ejeyV31 = maximovoltaje + 100
        ejeyV32 = minimovoltaje - 100




def graphCurrent1(list_fftCurrent,samplings): ##Grafica corriente CGE
        global labelscurrent1
        global valuescurrent1
        global ejeycurrent11
        global ejeycurrent12
        maximocorriente2sinmedia=getMaxValues(list_fftCurrent, 20)
        minimocorriente2sinmedia=getMinValues(list_fftCurrent, 20)
        maximocorriente = np.median(maximocorriente2sinmedia)
        minimocorriente = np.median(minimocorriente2sinmedia)
        valores = list_fftCurrent#[1000:4000]
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]
        #valores2=round(val,1)
        valuescurrent1 = [ i for i in valores ]
        #labelsI = [ i for i in range(0,len(values2)) ]
        labelscurrent1 = [ i for i in my_formatted_list ]
        ejeycurrent11 = maximocorriente +1
        ejeycurrent12 = minimocorriente -1




def graphCurrent2(list_fftCurrent,samplings): ##Grafica corriente Paneles
        global labelscurrent2
        global valuescurrent2
        global ejeycurrent21
        global ejeycurrent22
        maximocorriente2sinmedia=getMaxValues(list_fftCurrent, 20)
        minimocorriente2sinmedia=getMinValues(list_fftCurrent, 20)
        maximocorriente = np.median(maximocorriente2sinmedia)
        minimocorriente = np.median(minimocorriente2sinmedia)
        valores = list_fftCurrent#[1000:4000]
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]
        
        #valores2=round(val,1)
        valuescurrent2 = [ i for i in valores ]
        #labelsI = [ i for i in range(0,len(values2)) ]
        labelscurrent2 = [ i for i in my_formatted_list ]
        ejeycurrent21 = maximocorriente +1
        ejeycurrent22 = minimocorriente -1




def graphCurrent3(list_fftCurrent,samplings): ##Grafica corriente Carga
        global labelscurrent3
        global valuescurrent3
        global ejeycurrent31
        global ejeycurrent32
        maximocorriente2sinmedia=getMaxValues(list_fftCurrent, 20)
        minimocorriente2sinmedia=getMinValues(list_fftCurrent, 20)
        maximocorriente = np.median(maximocorriente2sinmedia)
        minimocorriente = np.median(minimocorriente2sinmedia)
        valores = list_fftCurrent#[1000:4000]
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        my_formatted_list = [ '%.2f' % elem for elem in tiempoms ]
        
        #valores2=round(val,1)
        valuescurrent3 = [ i for i in valores ]
        #labelsI = [ i for i in range(0,len(values2)) ]
        labelscurrent3 = [ i for i in my_formatted_list ]
        ejeycurrent31 = maximocorriente +1
        ejeycurrent32 = minimocorriente -1

#def graphCurrentjpg(list_fftVoltage, i):  #Grafica Corriente
       # y = np.linspace(0,len(list_fftVoltage),len(list_fftVoltage))
       # i = str(i)
       # x = list_fftVoltage*10
        #print(f'corriente fase : {i}')
       # plt.figure(figsize=(15, 5))
       # plt.plot(x)
       # oldepoch = time.time()
       # st = datetime.datetime.fromtimestamp(
       #     oldepoch).strftime('%Y-%m-%d-%H:%M:%S')
        #plt.title("Corriente Fase"+i+".", fontdict=font)
        #plt.ylabel("Corriente (mA-Peak-Peak)", fontdict=font)
       # plt.xlabel("Tiempo(s)", fontdict=font)
        # print("images/señal/current"+i+"/"+st+"Current"+i+".png")
        # plt.savefig("images/señal/current"+i+"/"+st+"Current"+i+".png")
        # plt.close(fig)



def graphFFTV1(list_fftVoltages, samplings):
        global valuesfftv1
        global labelsfftv1
        global largoejeyv1
        N = len(list_fftVoltages)
        T = 1 / samplings
        list_fftVoltages -= np.mean(list_fftVoltages)
        datosfft = list_fftVoltages * np.hamming(4096)
        yf = np.fft.rfft(datosfft)
       # yf=fft(list_fftVoltages)
        xf = fftfreq(N, T)[:N//2]  # tiene un largo de 2048
        #print(f'largo xf : {len(xf)}')
        mitad = samplings/2
        razon = mitad/2048
        intervalo = int(np.round(2600/razon,1))
        #ejey = np.abs(20*np.log10(yf[:N]))
        ejey = 2.0/N * np.abs(yf[0:N//2])
        valuesfftv1 = [ i for i in ejey]
        xf1 = np.round(xf,1)
        labelsfftv1 = [ i for i in xf1[:intervalo] ]
        largoejeyv1 = max(ejey)




def graphFFTV2(list_fftVoltages, samplings):
        global valuesfftv2
        global labelsfftv2
        global largoejeyv2
        N = len(list_fftVoltages)
        T = 1 / samplings
        list_fftVoltages -= np.mean(list_fftVoltages)
        datosfft = list_fftVoltages * np.hamming(4096)
        yf = np.fft.rfft(datosfft)
       # yf=fft(list_fftVoltages)
        xf = fftfreq(N, T)[:N//2]  # tiene un largo de 2048
        mitad = samplings/2
        razon = mitad/2048
        intervalo = int(np.round(2600/razon,1))
        ejey = 2.0/N * np.abs(yf[0:N//2])
        valuesfftv2 = [ i for i in ejey]
        xf1 = np.round(xf,1)
        labelsfftv2 = [ i for i in xf1[:intervalo] ]
        largoejeyv2 = max(ejey)



def graphFFTV3(list_fftVoltages, samplings):
        global valuesfftv3
        global labelsfftv3
        global largoejeyv3
        N = len(list_fftVoltages)
        T = 1 / samplings
        list_fftVoltages -= np.mean(list_fftVoltages)
        datosfft = list_fftVoltages * np.hamming(4096)
        yf = np.fft.rfft(datosfft)
       # yf=fft(list_fftVoltages)
        xf = fftfreq(N, T)[:N//2]  # tiene un largo de 2048
        mitad = samplings/2
        razon = mitad/2048
        intervalo = int(np.round(2600/razon,1))
        ejey = 2.0/N * np.abs(yf[0:N//2])
        valuesfftv3 = [ i for i in ejey]
        xf1 = np.round(xf,1)
        labelsfftv3 = [ i for i in xf1[:intervalo] ]
        largoejeyv3 = max(ejey)
        
        
    
def graphFFTI1(list_fftVoltages, samplings):
        global valuesffti1
        global labelsffti1
        global largoejeyi1
        N = len(list_fftVoltages)
        T = 1 / samplings
        list_fftVoltages -= np.mean(list_fftVoltages)
        datosfft = list_fftVoltages * np.hamming(4096)#np.kaiser(N,100)
        yf = np.fft.rfft(datosfft)
        #yf=fft(list_fftVoltages)
        mitad = samplings/2
        razon = mitad/((N/2))
        intervalo = int(np.round(2600/razon,1))
        # print(f'largo yf : {len(yf)}')
        xf = fftfreq(N, T)[:N//2]  # tiene un largo de 4096
        # print(f'largo xf : {len(xf)}')
        ejey = 2.0/N * np.abs(yf[0:N//2])
        #ejey = np.abs(20*np.log10(yf[:N]))
        #print(f'ejey: {ejey}')    
        valuesffti1 = [ i for i in ejey]
        xf1 = np.round(xf,1)
        labelsffti1 = [ i for i in xf1[:intervalo] ]
        largoejeyi1 = max(ejey)




def graphFFTI2(list_fftVoltages, samplings):
        global valuesffti2
        global labelsffti2
        global largoejeyi2
        N = len(list_fftVoltages)
        T = 1 / samplings
        list_fftVoltages -= np.mean(list_fftVoltages)
        datosfft = list_fftVoltages * np.hamming(4096)#np.kaiser(N,100)
        yf = np.fft.rfft(datosfft)
        #yf=fft(list_fftVoltages)
        mitad = samplings/2
        razon = mitad/((N/2))
        intervalo = int(np.round(2600/razon,1))
        # print(f'largo yf : {len(yf)}')
        xf = fftfreq(N, T)[:N//2]  # tiene un largo de 4096
        # print(f'largo xf : {len(xf)}')
        ejey = 2.0/N * np.abs(yf[0:N//2])
        #ejey = np.abs(20*np.log10(yf[:N]))
        #print(f'ejey: {ejey}')
        
        valuesffti2 = [ i for i in ejey]
        xf1 = np.round(xf,1)
        labelsffti2 = [ i for i in xf1[:intervalo] ]
        largoejeyi2 = max(ejey)




def graphFFTI3(list_fftVoltages, samplings):
        global valuesffti3
        global labelsffti3
        global largoejeyi3
        N = len(list_fftVoltages)
        T = 1 / samplings
        list_fftVoltages -= np.mean(list_fftVoltages)
        datosfft = list_fftVoltages * np.hamming(4096)#np.kaiser(N,100)
        yf = np.fft.rfft(datosfft)
        #yf=fft(list_fftVoltages)
        mitad = samplings/2
        razon = mitad/((N/2))
        intervalo = int(np.round(2600/razon,1))
        # print(f'largo yf : {len(yf)}')
        xf = fftfreq(N, T)[:N//2]  # tiene un largo de 4096
        # print(f'largo xf : {len(xf)}')
        ejey = 2.0/N * np.abs(yf[0:N//2])
        #ejey = np.abs(20*np.log10(yf[:N]))
        #print(f'ejey: {ejey}')
        valuesffti3 = [ i for i in ejey]
        xf1 = np.round(xf,1)
        labelsffti3 = [ i for i in xf1[:intervalo] ]
        largoejeyi3 = max(ejey)



DATVoltajeCGE=0.0
phasevoltajeCGE=0.0
FDvoltajeCGE=0.0
DATVoltajePaneles=0.0
phasevoltajePaneles=0.0
FDvoltajePaneles=0.0
DATVoltajeCarga=0.0
phasevoltajeCarga=0.0
FDvoltajeCarga=0.0


def VoltageFFT(list_fftVoltages, samplings,i):
    global j
    j = str(i)
    global DATVoltajeCGE
    global phasevoltajeCGE
    global FDvoltajeCGE
    global DATVoltajePaneles
    global phasevoltajePaneles
    global FDvoltajePaneles
    global DATVoltajeCarga
    global phasevoltajeCarga
    global FDvoltajeCarga
    
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
           #print(f'm2: {ynew[450:550]} ')
            #ejeyabsolut =  2.0/N * abs(ynew)
            #valuesfftv = [ i for i in ynew ]
            #labelsfftv = [ i for i in range(0,1000) ]
            #n = 0
            #ax = fig.add_subplot(111)
            #ax.plot(xnew,ynew)
            #rangex = np.zeros(56)
            #for h in range(0, 2600, 50):
            #     rangex[n]=h
            #     n = n+1
            #ax.xaxis.set_ticks(rangex)
            #ax.grid(True)
            #plt.title("FFT Voltaje Fase 1",fontdict=font)
            #ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
            #ax.set_ylabel('|dB|',fontdict=font)
            #oldepoch = time.time()
            #st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S')
            #plt.savefig("images/fft/voltage/"+st+"Voltage1.png")
            #z=0
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
           print(f'Vrms total: {round(SumMagnitudEficaz,2)}')
           Magnitud1 = FD[0]
           print(f'V rms armonico 1: {round(Magnitud1,2)}')
           #razon=Magnitud1/SumMagnitudEficaz
           #armonico1voltaje=valor*razon
           #print(f'FD Voltaje: {round(FD,2)}')
           #DATVoltaje = np.sqrt((valor**2-armonico1voltaje**2)/(armonico1voltaje**2))

           #sincvoltaje1 = 0
           if(j=="1"):
                 global sincvoltaje1
                 phasevoltajeCGE = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDvoltajeCGE = Magnitud1/SumMagnitudEficaz
                 DATVoltajeCGE = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 sincvoltaje1 = 1
                 #return phasevoltajeCGE,FDvoltajeCGE,DATVoltajeCGE

           #sincvoltaje2 = 0
           if(j=="2"):
                 global sincvoltaje2
                 phasevoltajePaneles = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDvoltajePaneles = Magnitud1/SumMagnitudEficaz
                 DATVoltajePaneles = np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 sincvoltaje2 = 1
                 #return phasevoltajePaneles,FDvoltajePaneles,DATVoltajePaneles

           #sincvoltaje3 = 0
           if(j=="3"):
                 global sincvoltaje3
                 phasevoltajeCGE = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDvoltajeCarga = Magnitud1/SumMagnitudEficaz
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
         #ejeyabsolut =  2.0/N * np.abs(ynew)
          # n = 0
          # ax = fig.add_subplot(111)
          # ax.plot(xnew,ynew)
          # rangex = np.zeros(56)
          # for h in range(0, 2600, 50):
          #  rangex[n]=h
          #  n = n+1
          # ax.xaxis.set_ticks(rangex)
          # ax.grid(True)
          # plt.title("FFT Corriente Fase"+i+".",fontdict=font)
          # ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
          # ax.set_ylabel('|dB|',fontdict=font)
          # oldepoch = time.time()
          # st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S')
          # plt.savefig("images/fft/current"+i+"/"+st+"Current"+i+".png")
          # print(f'i : {i}')
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
         print(f'Irms total: {round(SumMagnitudEficaz,2)}')
         Magnitud1 = FD[0]
         print(f'Irms armonico 1: {round(Magnitud1,2)}')
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
             print(f'FDCorrientePaneles : {FDCorrientePaneles }')
             DATCorrientePaneles = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             phasecorrientePaneles = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje2 == 1):
                 FPPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)*FDCorrientePaneles
                 cosphiPaneles=np.cos(phasevoltajePaneles-phasecorrientePaneles)
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 print(f'FP1 paneles: {FPPaneles}')
                 print(f'cos(phi) paneles : {cosphiPaneles}')
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
                 print(f'FP carga : {FPCarga}')
                 print(f'cos(phi) carga : {cosphiCarga}')
                 sincvoltaje3=0  
                 #return FPCGE
         #sincvoltaje3=0 
         
           
         #print(f'sincvoltaje == {sincvoltaje}')    
         #print(f'FP2 : {np.cos(0-GradoArmonicoFundamentalCorriente)*FD}')
         #MagnitudTotalArmonicosCorriente=round(sum,3)
         #MagnitudTotalArmonicosRms=round(sumsqrt,3)
         #thd_final = (raiz/thd_array[0])
         #print(f'Distorsion armonica total de la corriente de carga : {round(dat,2)}')
         #if(p==1):
          #   print(f'thd corriente 1: {round(thd_final,2)}')
           #  return round(sumsqrt,2)





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
#def Potencias(vrms, irms, phi, i):
    # i = str i
 #   global a
    #global energyfase1
   # Aparente = vrms*irms
  #  Activa = np.abs(vrms*irms*np.cos(phi))
    #print(f'Activa fase 1: {round(Activa,2)}')
   # Reactiva = vrms*irms*np.sin(phi)
   # if(i == 1):
    #    b = datetime.datetime.now()
     #   delta=(((b - a).microseconds)/1000+((b - a).seconds)*1000)/10000000000
        #print(f'ms: {delta}')
      #  energyfase1 += Activa*delta*2.8
        #print(f'energy: {round(energyFase1,3)}')
      #  a = datetime.datetime.now()
        #print(f'Aparente fase 1: {round(Aparente)}')
        #print(f'Activa fase 1: {round(Activa,2)}')
        #print(f'Reactiva fase 1: {round(Reactiva)}')
        #return Aparente,Activa,Reactiva,energyfase1


def Potencias(i,irms,vrms):
    i = str(i)
    
    if(i=="1"):
          global a
          global energyCGEFase1
          global ActivaCGEFase1
          global AparenteCGEFase1
          global ReactivaCGEFase1
          AparenteCGEFase1 = vrms*irms
          ActivaCGEFase1= np.abs(vrms*irms*cosphiCGE)
          ReactivaCGEFase1 = vrms*irms*np.sin(phasevoltajeCGE-phasecorrienteCGE)
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


def received():
           while True:
                  
                  esp32_bytes = esp32.readline()
                  decoded_bytes = str(esp32_bytes[0:len(esp32_bytes)-2].decode("utf-8"))#utf-8
                  np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')
                  #print(f'largo array inicial: {len(np_array)}')
       
                  if (len(np_array) == 8402):
                        if (np_array[0] == 11):
                            global vrms1
                            global irms1
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


                            vrms1 = VoltajeRms(NoVoltageoffset2)
                            VoltageFFT(NoVoltageoffset2,samplings,1)
                            graphVoltage1(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                            graphFFTV1(NoVoltageoffset2,samplings)
                            
                            
                            irms1 = CorrienteRms(NoCurrentoffset2)
                            CurrentFFT(NoCurrentoffset2,samplings,1)
                            graphCurrent1(NoCurrentoffset2,samplings)
                            graphFFTI1(NoCurrentoffset2,samplings)
                            #maximo=max(list_FPCurrent[1000:1700])
                            #minimo=min(list_FPCurrent[1000:1700])
                            #diferencia=maximo-minimo
                            #maximo2=max(list_FPCurrent)
                            #escalaI = valor1*np.sqrt(2) / maximo2
                            #listEscalaI=list_FPCurrent*escalaI
                            #samplings = np_array[-1]
                            graphVoltageCurrent(NoVoltageoffset,NoCurrentoffset,samplings)
                            Potencias(1,irms1,vrms1)
                            print(f'samplings 1: {samplings}')
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
                            graphVoltage2(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                            graphFFTV2(NoVoltageoffset2,samplings)
                            
                            
                            irms2 = CorrienteRms(NoCurrentoffset2)
                            CurrentFFT(NoCurrentoffset2,samplings,2)
                            graphCurrent2(NoCurrentoffset2,samplings)
                            graphFFTI2(NoCurrentoffset2,samplings)
                            #maximo=max(list_FPCurrent[1000:1700])
                            #minimo=min(list_FPCurrent[1000:1700])
                            #diferencia=maximo-minimo
                            #maximo2=max(list_FPCurrent)
                            #escalaI = valor1*np.sqrt(2) / maximo2
                            #listEscalaI=list_FPCurrent*escalaI
                            #samplings = np_array[-1]
                            graphVoltageCurrent(NoVoltageoffset,NoCurrentoffset,samplings)
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
                            graphVoltage3(NoVoltageoffset2,maximovoltaje2,minimovoltaje2,samplings)
                            graphFFTV3(NoVoltageoffset2,samplings)
                            
                            
                            irms3 = CorrienteRms(NoCurrentoffset2)
                            CurrentFFT(NoCurrentoffset2,samplings,3)
                            graphCurrent3(NoCurrentoffset2,samplings)
                            graphFFTI3(NoCurrentoffset2,samplings)
                            #maximo=max(list_FPCurrent[1000:1700])
                            #minimo=min(list_FPCurrent[1000:1700])
                            #diferencia=maximo-minimo
                            #maximo2=max(list_FPCurrent)
                            #escalaI = valor1*np.sqrt(2) / maximo2
                            #listEscalaI=list_FPCurrent*escalaI
                            #samplings = np_array[-1]
                            graphVoltageCurrent(NoVoltageoffset,NoCurrentoffset,samplings)
                            Potencias(3,irms3,vrms3)
                            print(f'samplings 3: {samplings}')
                            #FP(list_FPVoltage, list_FPCurrent, i=1)
                    
                  if (len(np_array)>0 and len(np_array)<=2):
                          global tempESP32
                          getTEMP()
                          #temphum()
                          #distance()
                          tempESP32 = round(np_array[0],0)
                          #print(f'array: {np_array}')
"""                           
                  if (len(np_array)==5202):            
                        if (np_array[0]==111):
                             samplings = np_array[-1]
                             print(f'Sampling voltage: {samplings}')
                             listEscalaV= (np_array[500:4596])#*0.36
                             #con media
                                   #valormaximovoltajesinmedia=getMaxValues(list_fftVoltage, 10)
                                   #maximo= np.median(valormaximovoltajesinmedia)
                                   #print(f'maximo voltaje fft 1 : {maximo}')
                                   #escalaV = valor*np.sqrt(2) / maximo
                                   #listEscalaV=list_fftVoltage*escalaV
                             #listEscalaV=(99*list_fftVoltage+list_fftVoltage)/100
                             #maximo2=max(listEscalaV)
                             #print(f'maximo voltaje fft 2: {maximo}')
                             print(f'maximo voltaje: {max(listEscalaV)}')
                             VoltageFFT(listEscalaV,samplings)
                             graphVoltage(listEscalaV)
                             graphFFT(listEscalaV,samplings)
                             #print(f'fft done')
                        if (np_array[0]==222):
                             samplings = np_array[-1]
                             print(f'Sampling corriente: {samplings}')
                             listEscalaI = np_array[500:4596]#/395
                             #con media
                                #valormaximocorrientesinmedia=getMaxValues(list_fftVoltage, 10)
                                #maximo2= np.median(valormaximocorrientesinmedia)
                                #print(f'maximo corriente fft: {maximo2}')
                                #escalaI = valor1*np.sqrt(2) / maximo2
                                #listEscalaI=list_fftVoltage*escalaI
                                #maximo2=max(listEscalaI)
                             #listEscalaI=(99*list_fftVoltage+list_fftVoltage)/100
                             print(f'maximo corriente: {max(listEscalaI)}')
                             CurrentFFT(listEscalaI,samplings,1)
                             graphCurrent(listEscalaI)
                             graphFFTI(listEscalaI,samplings)
                             #print(f'max current 2: {max(list_fftVoltage)}')
                             #print(f'max current 2: {max(listEscalaI)}')
"""

                  #if (len(np_array)>2 and len(np_array)<10):
                   #     global ApFase1
                    #    global AcFase1
                     #   global ReacFase1
                      #  global energyFase1
                        #valor = np_array[0]
                        #valor1 = np_array[1] 
                        #Potencias2(valor,valor1)
                        #print(f'time: {datetime.utcnow()}')
                        #print(f'vrms: {valor}')
                        #print(f'irms: {valor1}')
                        #if (phi1==None):
                        #     break
                        #else:
                         #    ApFase1,AcFase1,ReacFase1,energyFase1=Potencias(vrms=valor,irms=valor1,phi=phi1,i=1)
                        

x = datetime.datetime.now()

@app.route('/index.html')
def index():

     # For each pin, read the pin state and store it in the pins dictionary:
     for pin in pins:
          pins[pin]['state'] = GPIO.input(pin)
     # Put the pin dictionary into the template data dictionary:
     templateData = {
      'pins' : pins
      }

      #Muestra de for para graficar en pag
         #labels = x
         #print(f'labels:')
         #values = y
         #print(labels)
         #data = [
         #("1-1", 2),
         #("1-2", 0),
         #("1-3", 6),
         #("1-4", 8),
         #("1-5", 14),
         #("1-6", 6),
         #("1-7", 2),
         #]
         #print(data)
         #labels = [row[0] for row in data]
         #values = [row[1] for row in data]
     
      
     return render_template('index.html',**templateData,
      puerta="abierta",
      tempESP32=tempESP32,
      CPU_temp=CPU_temp,
      humedad=humedad,
      temperatura=temperatura,
      ip_address=ip_address,current_time=x.strftime("%c")
      )   

     


@app.route('/fase1.html')
def fase1():
     
     
     return render_template('fase1.html',
         vrms1=round(vrms1,2),
         irms1=round(irms1,2),
         AparenteCGEFase1=round(AparenteCGEFase1,2),
         ActivaCGEFase1=round(ActivaCGEFase1,2),
         ReactivaCGEFase1=round(ReactivaCGEFase1,2),
         vrms2=round(vrms2,2),
         irms2=round(irms2,2),
         AparentePanelesFase1=round(AparentePanelesFase1,2),
         ActivaPanelesFase1=round(ActivaPanelesFase1,2),
         ReactivaPanelesFase1=round(ReactivaPanelesFase1,2),
         vrms3=round(vrms3,2),
         irms3=round(irms3,2),
         AparenteCargaFase1=round(AparenteCargaFase1,2),
         ActivaCargaFase1=round(ActivaCargaFase1,2),
         ReactivaCargaFase1=round(ReactivaCargaFase1,2),
         energyCGEFase1=round(energyCGEFase1,2),
         energyPanelesFase1=round(energyPanelesFase1,2),
         energyCargaFase1=round(energyCargaFase1,2),
         DATVoltajeCGE=round(DATVoltajeCGE,2),
         phasevoltajeCGE=round(phasevoltajeCGE,2),
         FDvoltajeCGE=round(FDvoltajeCGE,2),
         DATVoltajePaneles=round(DATVoltajePaneles,2),
         phasevoltajePaneles=round(phasevoltajePaneles,2),
         FDvoltajePaneles=round(FDvoltajePaneles,2),
         DATVoltajeCarga=round(DATVoltajeCarga,2),
         phasevoltajeCarga=round(phasevoltajeCarga,2),
         FDvoltajeCarga=round(FDvoltajeCarga,2),
         FDCorrienteCGE=round(FDCorrienteCGE,2),
         DATCorrienteCGE =round(DATCorrienteCGE,2), 
         phasecorrienteCGE=round(phasecorrienteCGE,2),
         FDCorrientePaneles=round(FDCorrientePaneles,2),
         DATCorrientePaneles=round(DATCorrientePaneles,2),
         phasecorrientePaneles=round(phasecorrientePaneles,2),
         FDCorrienteCarga=round(FDCorrienteCarga,2),
         DATCorrienteCarga=round(DATCorrienteCarga,2),
         phasecorrienteCarga=round(phasecorrienteCarga,2),
         FPCGE=round(FPCGE,2),
         cosphiCGE=round(cosphiCGE,2),
         FPPaneles=round(FPPaneles,2),
         cosphiPaneles=round(cosphiPaneles,2),
         FPCarga=round(FPCarga,2),
         cosphiCarga=round(cosphiCarga,2),
         labelsvoltaje1=labelsvoltaje1,
         labelsvoltaje2=labelsvoltaje2,
         labelsvoltaje3=labelsvoltaje3,
         valuesvoltaje1=valuesvoltaje1,
         valuesvoltaje2=valuesvoltaje2,
         valuesvoltaje3=valuesvoltaje3,
         ejeyV11=ejeyV11,
         ejeyV12=ejeyV12,
         ejeyV21=ejeyV21,
         ejeyV22=ejeyV22,
         ejeyV31=ejeyV31,
         ejeyV32=ejeyV32,
         labelscurrent1=labelscurrent1,
         valuescurrent1=valuescurrent1,
         ejeycurrent11=ejeycurrent11,
         ejeycurrent12=ejeycurrent12,
         labelscurrent2=labelscurrent2,
         valuescurrent2=valuescurrent2,
         ejeycurrent21=ejeycurrent21,
         ejeycurrent22=ejeycurrent22,
         labelscurrent3=labelscurrent3,
         valuescurrent3=valuescurrent3,
         ejeycurrent31=ejeycurrent31,
         ejeycurrent32=ejeycurrent32,
         labelsfftv1=labelsfftv1,
         valuesfftv1=valuesfftv1,
         largoejeyv1=largoejeyv1,
         labelsfftv2=labelsfftv2,
         valuesfftv2=valuesfftv2,
         largoejeyv2=largoejeyv2,
         labelsfftv3=labelsfftv3,
         valuesfftv3=valuesfftv3,
         largoejeyv3=largoejeyv3,
         labelsffti1=labelsffti1,
         valuesffti1=valuesffti1,
         largoejeyi1=largoejeyi1,
         labelsffti2=labelsffti2,
         valuesffti2=valuesffti2,
         largoejeyi2=largoejeyi2,
         labelsffti3=labelsffti3,
         valuesffti3=valuesffti3,
         largoejeyi3=largoejeyi3,
         tempESP32=tempESP32,
         CPU_temp=CPU_temp,
         humedad=humedad,
         temperatura=temperatura,
         ip_address=ip_address,
         #maxcorriente=maxcorriente,
         #mincorriente=mincorriente,
         labelsfp=labelsfp,
         valuesvoltage=valuesvoltage,
         valuescurrent=valuescurrent,
         maxvoltaje=maxvoltaje,
         minvoltaje=minvoltaje)   
    

@app.route('/<changePin>/<action>')
def action(changePin, action):

     
     # Convert the pin from the URL into an integer:
     changePin = int(changePin)
     # Get the device name for the pin being changed:
     deviceName = pins[changePin]['name']
     # If the action part of the URL is "on," execute the code indented below:
     if action == "on":
        # Set the pin high:
        GPIO.output(changePin, GPIO.HIGH)
        # Save the status message to be passed into the template:
        message = "Turned " + deviceName + " on."
     if action == "off":
        GPIO.output(changePin, GPIO.LOW)
        message = "Turned " + deviceName + " off."
  
     # For each pin, read the pin state and store it in the pins dictionary:
     for pin in pins:
        pins[pin]['state'] = GPIO.input(pin)
  
     # Along with the pin dictionary, put the message into the template data dictionary:
     templateData = {
        'pins' : pins
     }

     return render_template('index.html', **templateData, puerta=puerta,
      tempESP32=tempESP32,
      CPU_temp=CPU_temp,
      humedad=humedad,
      temperatura=temperatura,
      ip_address=ip_address)
  
  

if __name__ == '__main__':

    t = threading.Thread(target=received)
    t.daemon = True
    t.start()
    app.run(debug=False)
