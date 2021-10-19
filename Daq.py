from tkinter import *
from timeit import default_timer 
from time import sleep
import signal
import sys
import time
from tkinter import filedialog
from PIL import Image
from PIL import ImageTk
import cv2
import imutils
import numpy as np
import glob
import os
import serial
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
plt.style.use('ggplot')
from PIL import ImageTk,Image
import serial, time, csv, ast,datetime
import numpy as np
import pandas as pd
from scipy import signal
from scipy.interpolate import lagrange
from scipy.fft import fft, fftfreq
from scipy import interpolate
import matplotlib
import datetime
from datetime import timedelta
import time
from os import remove



root = Tk()
root.geometry("500x500")
root.title("Medidor SER")


esp32 = serial.Serial('/dev/ttyUSB0', 230400, timeout=0.5)
esp32.flushInput()


if not os.path.exists('log/'):
    os.makedirs('log/')

if not os.path.exists('images/'):
    os.makedirs('images/')

if not os.path.exists('images/fft'):
    os.makedirs('images/fft')

if not os.path.exists('images/fft/voltage'):
    os.makedirs('images/fft/voltage')

if not os.path.exists('images/fft/current'):
    os.makedirs('images/fft/current')


if not os.path.exists('images/señal'):
    os.makedirs('images/señal') 

if not os.path.exists('images/señal/voltage'):
    os.makedirs('images/señal/voltage') 
   

font = {'family': 'serif',
        'color':  'darkred',
        'weight': 'normal',
        'size': 8,
        }


folder = "images/señal/voltage/"
def BorrarArchivos():
      count1=0
      count2=0
      count3=0
      for f in os.listdir("images/señal/voltage/"):
                 #print(f)
                 count1=count1+1
      for f in os.listdir("images/fft/current/"):
                 #print(f)
                 count2=count2+1
      for f in os.listdir("images/fft/voltage/"):
                 #print(f)
                 count3=count3+1
        #print(count)
      try:
          if(count2>10):
               for i in os.listdir("images/señal/voltage"):
                    print(i)
                    os.remove(f'images/señal/voltage/{i}')
          if(count2>10):
               for i in os.listdir("images/fft/current"):
                    print(i)
                    os.remove(f'images/fft/current/{i}')    
          if(count3>10):
               for i in os.listdir("images/fft/voltage"):
                    print(i)
                    os.remove(f'images/fft/voltage/{i}')          
                       
                       
      except OSError: 
               print("Imagen does not exists or is inaccessible")
               pass



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
        

def graphVoltage(list_fftVoltage,list_FPCurrent,samplings):
        #y = np.linspace(0,len(list_fftVoltage),1000)
        #x = len(list_fftVoltage)
        #print(f'largo : {x}') 
        global render
        #plt.figure(figsize=(12,2))
        #plt.plot(list_fftVoltage)
        fig=plt.figure(figsize=(12,3))
        #plt.plot(list_fftVoltage, color="blue", label="Voltaje")
        #plt.plot(list_FPCurrent, color="green", label="Corriente")
        #plt.legend(loc='upper left')
        tiempo = 1/(samplings*(0.001/4200))
        tiempoms = np.arange(0,tiempo,tiempo/4096)
        ax = fig.add_subplot(111)
        ax.plot(tiempoms,list_fftVoltage,color="blue", label="Voltaje")
        ax.plot(tiempoms,list_FPCurrent,color="green", label="Corriente")
        ax.legend(loc='upper left')
        #st=datetime.datetime.minute
        oldepoch = time.time()
        st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S') 
        #plt.xlabel("Tiempo(ms)",fontdict=font)
        ax.set_xlabel('Tiempo (mS)',fontdict=font)
        ax.set_ylabel('Pk-Pk',fontdict=font) 
        imagenVoltaje = f'images/señal/voltage/{st}-Voltage.png'
        plt.savefig(imagenVoltaje)
        load = Image.open(imagenVoltaje)
        render = ImageTk.PhotoImage(load)
        Label(root, image=render).grid(column=2, row=0, rowspan=6)
        
        
        #plt.savefig(imagenVoltaje)
        #plt.close(fig)

def graphCurrent(list_fftVoltage, i):
       # y = np.linspace(0,len(list_fftVoltage),len(list_fftVoltage))
        i = str(i)
        x = list_fftVoltage*10
        print(f'corriente fase : {i}') 
        plt.figure(figsize=(15,5))
        plt.plot(x)
        oldepoch = time.time()
        st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S') 
        plt.title("Corriente Fase"+i+".",fontdict=font)
        plt.ylabel("Corriente (mA-Peak-Peak)",fontdict=font)
        plt.xlabel("Tiempo(s)",fontdict=font)
        #print("images/señal/current"+i+"/"+st+"Current"+i+".png")
        plt.savefig("images/señal/current"+i+"/"+st+"Current.png")
        #plt.close(fig)

                       

    
def graphVoltageFFT(list_fftVoltages,samplings):
    N = 4096   
    T = 1 / samplings    
    yf = fft(list_fftVoltages)
    Label(root, text=round(samplings,2),font=('Arial', 16)).grid(row=10, column=1)
    print(f'largo yf : {len(yf)}')
    xf = fftfreq(N, T)[:N] #tiene un largo de 4096
    print(f'largo xf : {len(xf)}')
    fig = plt.figure(figsize=(35,15))
    ejey = 20*np.log10(yf[:N])
    if (len(xf)>2550):
           f = interpolate.interp1d(xf, ejey)
           xnew = np.arange(0, 2550, 1) #2550
           print(f'largo xnew : {len(xnew)}')
           ynew = f(xnew) 
           n = 0
           ax = fig.add_subplot(111)
           ax.plot(xnew,ynew)
           rangex = np.zeros(56)
           for h in range(0, 2600, 50):
             rangex[n]=h
             n = n+1
           ax.xaxis.set_ticks(rangex)  
           ax.grid(True)
           plt.title("FFT Voltaje Fase 1",fontdict=font)
           ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
           ax.set_ylabel('|dB|',fontdict=font) 
           oldepoch = time.time()
           st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S')  
           plt.savefig("images/fft/voltage/"+st+"Voltage1.png")
           
           thd_array = []
           for i in range(46, 2596,50):
                a = np.amax(np.abs(ynew[i:i+8]))
                thd_array.append(a)
                #print(i)
                #print(a)
           sum = np.sum([thd_array[1:51]])     
           raiz = np.sqrt(sum)
           thd_final = (raiz/thd_array[0])     
           print(f'largo thd_array : {len(thd_array)}')
           print(f'thd_final : {thd_final}'+' %') 
           Label(root, text=round(thd_final,2),font=('Arial', 16)).grid(row=6, column=1) 


def graphCurrentFFT(list_fftVoltages,samplings):
    N = 4096   
    T = 1 / samplings
    yf = fft(list_fftVoltages)
    xf = fftfreq(N, T)[:N]
    #print(f'largo xf : {len(xf)}')
    fig = plt.figure(figsize=(35,15))
    ejey = 20*np.log10(yf[:N])
    if (len(xf)>2550):
         f = interpolate.interp1d(xf, ejey)
         xnew = np.arange(0, 2550, 1)
         #print(f'largo xnew : {len(xnew)}')
         ynew = f(xnew) 
         
         n = 0
         ax = fig.add_subplot(111)
         ax.plot(xnew,ejeyabsolut)
         rangex = np.zeros(56)
         for h in range(0, 2600, 50):
           rangex[n]=h
           n = n+1
         ax.xaxis.set_ticks(rangex)  
         ax.grid(True)
         plt.title("FFT Corriente Fase"+i+".",fontdict=font)
         ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
         ax.set_ylabel('|dB|',fontdict=font) 
         oldepoch = time.time()
         st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S')  
         plt.savefig("images/fft/current"+i+"/"+st+"Current"+i+".png")
         
         thd_array = []
         for i in range(46, 2596,50):
              a = np.amax(np.abs(ynew[i:i+8]))
              thd_array.append(a)
              #print(i)
              #print(a)
         sum = np.sum([thd_array[1:51]])     
         raiz = np.sqrt(sum)
         thd_final = (raiz/thd_array[0])     
     


                      
    

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
    return vrms

irms=0.0
def CurrentRms(maximocorriente2):
    if(maximocorriente2>430):
         irms = maximocorriente2*0.0133
    else:
         irms=(0.0046 + 0.000282*maximocorriente2 - 0.00000328*(maximocorriente2**2) + 0.0000000167*(maximocorriente2**3) - 0.0000000000382*(maximocorriente2**4) + 0.0000000000000322*(maximocorriente2**5))*maximocorriente2
    
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


def CalculoDesfase(NoVoltageoffset,NoCurrentoffset,samplings):
    try:
        tiempo = 1/(samplings*(0.001/4200))
        tiempo2=(tiempo/4200)
        peakind = signal.find_peaks_cwt(NoVoltageoffset,np.arange(1,100))
        peakind2 = signal.find_peaks_cwt(NoCurrentoffset,np.arange(1,100))
        peakindvoltajems = peakind* tiempo2
        peakindcorrientems = peakind2* tiempo2
        dif=(peakindvoltajems-peakindcorrientems)
        print(np.mean(dif[1:10]/8.333))
    except:
        pass
    






DATVoltajeCGE=0.0
phasevoltajeCGE=0.0
FDVoltajeCGE=0.0



def VoltageFFT(list_fftVoltages, samplings,i):
    global j
    j = str(i)
    global DATVoltajeCGE
    global phasevoltajeCGE
    global FDVoltajeCGE
    global renderfftVoltage
    
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
           n = 0
           fig = plt.figure(figsize=(12,2))
           ax = fig.add_subplot(111)
           ax.plot(xnew,ejeyabsolut)
           rangex = np.zeros(28)
           for h in range(50, 2600, 100):
             rangex[n]=h
             n = n+1
           ax.xaxis.set_ticks(rangex)  
           ax.grid(True)
           plt.title("FFT Voltaje",fontdict=font)
           ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
           ax.set_ylabel('|dB|',fontdict=font) 
           oldepoch = time.time()
           st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S')  
           imagenVoltajeFFT = f'images/fft/voltage/{st}-Voltagefft.png'
           plt.savefig(imagenVoltajeFFT)
           load = Image.open(imagenVoltajeFFT)
           renderfftVoltage = ImageTk.PhotoImage(load)
           Label(root, image=renderfftVoltage).grid(column=2, row=13, rowspan=6)
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
           if(j=="1"):
                 global sincvoltaje1            
                 phasevoltajeCGE = np.arctan(real[0]/(imag[0]))
                 #FaseArmonicoFundamentalVoltaje1=round(np.angle(complejo[0]),2)
                 FDVoltajeCGE = Magnitud1/SumMagnitudEficaz
                 DATVoltajeCGE1= np.sqrt(((SumMagnitudEficaz**2)-(Magnitud1**2))/(Magnitud1**2))
                 print(f'DAT Voltaje CGE: {round(DATVoltajeCGE1,2)}')
                 sincvoltaje1 = 1
                 
               


DATCorrienteCGE = 0.0
FDCorrienteCGE= 0.0
FDCorrienteCGE= 0.0
phasecorrienteCGE= 0.0
FPCGE= 0.99
FPCGE0= 0.99
cosphiCGE= 0.0
FDCorrienteCGE1=0.0
DATCorrienteCGE1=0.0


def CurrentFFT(list_fftVoltages, samplings, i,irms):
    global DATCorrienteCGE
    global DATCorrienteCGE1
    global a2
    global FDCorrienteCGE 
    global FDCorrienteCGE1
    global phasecorrienteCGE
    global FPCGE
    global FPCGE0
    global cosphiCGE
    global q
    global renderfftcurrent

   
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
         n = 0
         fig = plt.figure(figsize=(12,2))
         ax = fig.add_subplot(111)
         ax.plot(xnew,ejeyabsolut)
         rangex = np.zeros(28)
         for h in range(50, 2600, 100):
           rangex[n]=h
           n = n+1
         ax.xaxis.set_ticks(rangex)  
         ax.grid(True)
         plt.title("FFT Corriente Fase",fontdict=font)
         ax.set_xlabel('Frecuencia (Hz)',fontdict=font)
         ax.set_ylabel('|dB|',fontdict=font) 
         oldepoch = time.time()
         st = datetime.datetime.fromtimestamp(oldepoch).strftime('%Y-%m-%d-%H:%M:%S')  
         imagenCurrentFFT = f'images/fft/current/{st}-Currentfft.png'
         plt.savefig(imagenCurrentFFT)
         load = Image.open(imagenCurrentFFT)
         renderfftcurrent = ImageTk.PhotoImage(load)
         Label(root, image=renderfftcurrent).grid(column=2, row=7, rowspan=6)
         #p = int(i)
         #z=0
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
         #irmstotalproporcionado=np.sqrt((irmsarmonico1prop**2)+(ArmonicosRestantes*proporcion)**2)
         #MagnitudArmonicoFundamentalCorriente=round(thd_array[0],3)
         #fp2=round((armonico1corriente*np.cos(phasevoltaje-phasen))/valor1,2)
         #FaseArmonicoFundamentalCorriente=round(np.angle(complejo[0]),2)
         
         #GradoArmonicoFundamentalCorriente=round(Grados,2)
         if(q=="1"):
             global sincvoltaje1
             FDCorrienteCGE1 = irmsarmonico1prop/irms
             print(f'FDCorrienteCGE : {FDCorrienteCGE1 }')
             DATCorrienteCGE1 = np.sqrt((SumMagnitudEficaz**2-Magnitud1**2)/(Magnitud1**2))
             #print(f'DAT corriente CGE: {DATCorrienteCGE}')
             phasecorrienteCGE = np.arctan(real[0]/(imag[0]))
             if (sincvoltaje1 == 1):
                 FPCGE0=np.cos(phasevoltajeCGE-phasecorrienteCGE)*FDCorrienteCGE1
                 print(f'Desfase voltaje: {phasevoltajeCGE}')
                 print(f'Desfase corriente: {phasecorrienteCGE}')
                 print(f'Desfase: {phasevoltajeCGE-phasecorrienteCGE}')
                 cosphiCGE=np.cos(phasevoltajeCGE-phasecorrienteCGE)
                 if(FPCGE0>0.0):
                     FPCGE0=FPCGE0+0.05
                 else:
                     FPCGE0=FPCGE0-0.05
                 if(FPCGE0>=1.0):
                     FPCGE0=0.99
                 if(FPCGE0<=-1.0):
                     FPCGE0=-0.99
                
                 #FP=np.cos(FaseArmonicoFundamentalVoltaje-FaseArmonicoFundamentalCorriente)
                 print(f'FP1 cge: {round(FPCGE0,2)}')
                 print(f'cos(phi) cge : {cosphiCGE}')
                 sincvoltaje1=0  
                 #return FPCGE
         #sincvolaje1=0 




a = datetime.datetime.now()  
#energyCGEFase11 = 0.0
energyCGEFase11 = 0.0
energyCGEFase11Hour = 0.0
AparenteCGEFase11 = 0.0
ActivaCGEFase11 = 0.0
ReactivaCGEFase11 = 0.0
energyCGEFase1 = 0.0
AparenteCGEFase1 = 0.0
ActivaCGEFase1 = 0.0
ReactivaCGEFase1 = 0.0




def Potencias(irms,vrms):
          global a
          global energyCGEFase1
          global ActivaCGEFase1
          global AparenteCGEFase1
          global ReactivaCGEFase1
          global energyCGEFase11
          global energyCGEFase11Hour
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
          energyCGEFase11 += ActivaCGEFase11*delta*2.9
          energyCGEFase11Hour += ActivaCGEFase11*delta*2.9
          a = datetime.datetime.now()
          if(a2.minute==0 or a2.minute==1):
              energyCGEFase11Hour=0
          if(a2.hour==0 and a2.minute==0):
              energyCGEFase11=0
          if(a2.hour==0 and a2.minute==1):
              energyCGEFase11=0
          print(f'Energia CGE: {energyCGEFase11}')
     
    
    


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


def sin_wave(A, fs, N,phi):
    
    x = np.arange(N)
    y = A*np.sin(2*np.pi*50*x/fs + phi)
    #plt.plot(x, y)
    #plt.xlabel('sample(n)')
    #plt.ylabel('voltage(V)')
    #plt.show()
    return y

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
                       if (np_array[0] == 11):
                            global modamaximovoltaje11
                            global modamaximocorriente11
                            global vrms1
                            global vrms11
                            global irms1
                            global irms11
                            global modavoltaje
                            global modacorriente
                            global samplings
                            #fs=30000 
                            samplings = np_array[-1]
                            list_FPVoltage3 = np_array[0:4200]
                            list_FPCurrent3 = np_array[4201:8400]
                            #list_FPVoltage3 = sin_wave(A=320, fs=fs, N=4200, phi=0)#np_array[0:4200]
                            #list_FPCurrent3 = sin_wave(A=320, fs=fs,N=4200, phi=4.18) #np_array[4201:8400]
                            #print(f'max inicio: {max(list_FPVoltage3)}')
                            sos = signal.butter(10, 2500, 'low', fs=samplings, output='sos')
                            list_FPVoltage2 = signal.sosfilt(sos, list_FPVoltage3)
                            #list_FPVoltage2 = savgol_filter(list_FPVoltage2,len(list_FPVoltage2)-1,))
                            #sos = signal.butter(4, 50, 'low', fs=samplings, output='sos')
                            list_FPCurrent2 = signal.sosfilt(sos, list_FPCurrent3)
                            #print(f'max inicio con filtro: {max(list_FPVoltage2)}')
                            list_FPVoltage = list_FPVoltage2[104:4200]
                            list_FPCurrent = list_FPCurrent2[104:4200]
                            #Valor dc de Voltaje
                            
                            valoresmaximovoltajesinmedia=getMaxValues(list_FPVoltage, 50)
                            valoresminimovoltajesinmedia=getMinValues(list_FPVoltage, 50)
                            maximovoltaje = np.median(valoresmaximovoltajesinmedia)
                            minimovoltaje = np.median(valoresminimovoltajesinmedia)
                            mediadcvoltaje = (maximovoltaje+minimovoltaje)/2
                            # Valores maximo y minimos de voltaje sin componente continua
                            NoVoltageoffset=list_FPVoltage-mediadcvoltaje
                            
                            vrms1=VoltajeRms(NoVoltageoffset)
                            
                            if (len(modamaximovoltaje11)>=5):
                                modavoltaje=np.median(modamaximovoltaje11)
                                vrms1=VoltRms(modavoltaje)
                                print(f'Vrms CGE: {vrms1}')
                                VoltageFFT(NoVoltageoffset,samplings,1)
                                
                                modamaximovoltaje11=[]
                            else:
                                modamaximovoltaje11.append(vrms1)
                                #print(f'array voltaje: {modamaximovoltaje2
                            #Valor dc de corriente
                            
                            valoresmaxcorriente=getMaxValues(list_FPCurrent, 50)
                            valoresmincorriente=getMinValues(list_FPCurrent, 50)
                            maximocorriente = np.median(valoresmaxcorriente)
                            minimocorriente = np.median(valoresmincorriente)
                            mediadccorriente = (maximocorriente+minimocorriente)/2
                            
                            # Valores maximo y minimos de corriente
                            NoCurrentoffset=list_FPCurrent-mediadccorriente
                            
                            irms1=CorrienteRms(NoCurrentoffset)
            
                            if (len(modamaximocorriente11)>=5):
                                modacorriente=np.median(modamaximocorriente11)
                                irms1=CurrentRms(modacorriente)
                                print(f'Irms CGE: {irms1}')
                                CurrentFFT(NoCurrentoffset,samplings,1,irms1)
                                #print(f'MODA CORRIENTE CGE: {modacorriente}')
                                Potencias(irms1,vrms1)
                                modamaximocorriente11=[]
                                graphVoltage(NoVoltageoffset,NoCurrentoffset,samplings)
                                CalculoDesfase(NoVoltageoffset,NoCurrentoffset,samplings)
                                #BorrarArchivos() 
                            else:
                                modamaximocorriente11.append(irms1)
                            #    print(f'array corriente: {modamaximocorriente2}')
            Label(root, text=round(vrms1,2),font=('Arial', 16)).grid(row=1, column=1)
            Label(root, text=round(irms1,2),font=('Arial', 16)).grid(row=2, column=1)
            Label(root, text=round(energyCGEFase11,2),font=('Arial', 16)).grid(row=3, column=1)
            Label(root, text=round(ActivaCGEFase11,4),font=('Arial', 16)).grid(row=4, column=1)
            Label(root, text=round(AparenteCGEFase11,2),font=('Arial', 16)).grid(row=5, column=1)
            Label(root, text=round(ReactivaCGEFase11,2),font=('Arial', 16)).grid(row=6, column=1)
            Label(root, text=round(FPCGE0,2),font=('Arial', 16)).grid(row=7, column=1)
            Label(root, text=round(cosphiCGE,2),font=('Arial', 16)).grid(row=8, column=1)
            
            BorrarArchivos()
            root.after(2000, received) 
            

                
                 
                 
             


#show_imagen_voltaje()
#show_imagen_voltaje_fft()
#lblInputImage.image = ""
#lblInputImage2.image = ""

"""
lblInputImage = Label(root)
lblInputImage.grid(column=2, row=1, rowspan=6)
lblInputImage2 = Label(root)
lblInputImage2.grid(column=2, row=8, rowspan=6) 
"""
selected = IntVar()

#my_buttonCurrentFFT = Button(root, text="FFT Corriente", command=graphCurrentFFT)
#my_buttonCurrentFFT.grid(row=4,column=2)

Label(root, text="Voltaje",font=('Arial', 16)).grid(row=1, column=0)
Label(root, text="Corriente",font=('Arial', 16)).grid(row=2, column=0)
Label(root, text="Energia",font=('Arial', 16)).grid(row=3, column=0)
Label(root, text="Activa",font=('Arial', 16)).grid(row=4, column=0)
Label(root, text="Aparente",font=('Arial', 16)).grid(row=5, column=0)
Label(root, text="Reactiva",font=('Arial', 16)).grid(row=6, column=0)
Label(root, text="FP",font=('Arial', 16)).grid(row=7, column=0)
Label(root, text="cos phi",font=('Arial', 16)).grid(row=8, column=0)



root.after(2000, received)
root.mainloop()