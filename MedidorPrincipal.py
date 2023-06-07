import requests
from datetime import date, datetime, time, timedelta
from datetime import datetime
import json
import shutil
from openpyxl.chart import (
    AreaChart,
    BarChart,
    Reference,
    Series,
    PieChart,
    ProjectedPieChart,
    Reference
)
import gspread
import xlsxwriter
import random
import time
import paho.mqtt.client as mqtt
import os
import datetime
from scipy import interpolate
from scipy.fft import fft, fftfreq
from scipy.fft import rfft, rfftfreq
from scipy.interpolate import lagrange
from scipy import signal
from scipy.signal import savgol_filter
import numpy as np
import subprocess
#import RPi.GPIO as GPIO
import sys
import time
import os
import serial
import openpyxl
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import datetime
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import argparse
import psutil
from DataSave import Maximo15min
import threading
#from connector import iot_ser_db
#from data_db import list_data_db_insert
from mqtt_helper import get_mqtt_credentials, on_disconnect, on_connected, ConnectToBroker, userId, dId



k1="REDCompañia"	
k2="CentralFotovoltaica"	
k3="ConsumoCliente"	
f1="Fase-1"	
f2="Fase-2"	
f3="Fase-3"

parser = argparse.ArgumentParser(description="Análisis de señales digitales")
parser.add_argument("--use_tkinter", action="store_true", help="Habilitar la interfaz gráfica Tkinter")
parser.add_argument("--use_serial", action="store_true", help="Habilitar la lectura de datos desde el puerto serie")
parser.add_argument("--use_mqtt", action="store_true", help="Habilitar la conexión con el broker MQTT")
parser.add_argument("--use_mongo", action="store_true", help="Habilitar el guardado en mongo")
parser.add_argument("--get_devices", action="store_true", help="Obtener devices")
parser.add_argument('--largo', type=int, default=9, help='Largo de la lista (predeterminado: 9).')
args = parser.parse_args()
largo_lista = args.largo

if largo_lista == 3:
    Variable_list = ["carga_fase_1","paneles_fase_1","red_fase_1"]
elif largo_lista == 6:
    Variable_list = ["carga_fase_1","paneles_fase_1","red_fase_1","carga_fase_2","paneles_fase_2","red_fase_2"]
elif largo_lista == 9:
    Variable_list = ["carga_fase_1","paneles_fase_1","red_fase_1","carga_fase_2","paneles_fase_2","red_fase_2","carga_fase_3","paneles_fase_3","red_fase_3"]

def get_extreme_values(my_list, quantity, max_or_min):
    return sorted(set(my_list), reverse=max_or_min)[:quantity]

def get_max_values(my_list, quantity):
    return get_extreme_values(my_list, quantity, True)

def get_min_values(my_list, quantity):
    return get_extreme_values(my_list, quantity, False)


def print_memory_usage():
    mem = psutil.virtual_memory()
    print("Memoria RAM usada: {:.2f}%".format(mem.percent))

def VoltajeRms(listVoltage):
    N = len(listVoltage)
    Squares = [v ** 2 for v in listVoltage]
    MeanSquares = sum(Squares) / N
    Vrms = np.sqrt(MeanSquares)
    return Vrms

def CorrienteRms(listCurrent):
    N = len(listCurrent)
    Squares = [i ** 2 for i in listCurrent]
    MeanSquares = sum(Squares) / N
    Irms = np.sqrt(MeanSquares)
    return Irms

def PotenciaRms(listCurrent, listVoltage):
    N = len(listCurrent)
    Squares = [listCurrent[i] * listVoltage[i] for i in range(N)]
    MeanSquares = sum(Squares) / N
    return MeanSquares

def butter_lowpass_filter(datas, cutoff, fs, order=10):
    sos = signal.butter(order, cutoff, 'low', fs=fs, output='sos')
    return signal.sosfilt(sos, datas)


def plot_signal_and_fft(signal, xf, amplitudes, index, fig, ax1, ax2,SeñalType,use_tkinter=True):
    if use_tkinter:
        # Limpiar los ejes de los gráficos
        ax1.clear()
        ax2.clear()
    
        # Filtrar solo los primeros 100 armónicos
        num_harmonics = 100
        xf_filtered = xf
        amplitudes_filtered = amplitudes
        print(len(signal))
        print(len(xf))
        print(len(amplitudes))
        # Graficar la señal de entrada en el primer subplot
        ax1.plot(signal)
        ax1.set_title(f'Señal de {SeñalType} - Índice: {index}')
        ax1.set_xlabel('Tiempo [muestras]')
        ax1.set_ylabel('Amplitud')
    
        # Graficar la FFT de la señal en el segundo subplot
        ax2.plot(xf_filtered, amplitudes_filtered)
        ax2.set_title('FFT de la señal')
        ax2.set_xlabel('Frecuencia [Hz]')
        ax2.set_ylabel('Amplitud')
        ax2.set_xlim(0, 2600)  # Establecer el límite del eje x
    
        # Encontrar los picos más grandes cercanos a cada armónico múltiplo de 50
        search_range = 25
        harmonic_magnitudes = {}
        for harmonic in range(1, num_harmonics + 1):
            x = harmonic * 50
            lower_bound = max(0, x - search_range)
            upper_bound = min(len(xf_filtered), x + search_range)
            max_magnitude = -1
            for i in range(lower_bound, upper_bound):
                if amplitudes_filtered[i] > max_magnitude:
                    max_magnitude = amplitudes_filtered[i]
                    harmonic_magnitudes[x] = max_magnitude
    
        # Ordenar los armónicos por magnitud en orden descendente
        sorted_harmonics = sorted(harmonic_magnitudes.items(), key=lambda x: x[1], reverse=True)
    
        # Seleccionar solo los N armónicos con las magnitudes más grandes
        num_top_harmonics = 10
        top_harmonics = sorted_harmonics[:num_top_harmonics]
    
        margin = 100
        last_harmonic_x = top_harmonics[-1][0]
        ax2.set_xlim(0, last_harmonic_x + margin)
    
    
        # Agregar anotaciones y etiquetas solo para los armónicos con las magnitudes más grandes
        for x, magnitude in top_harmonics:
            y = magnitude
            ax2.annotate(f"{y:.2f}", xy=(x, y), xytext=(x, y + 0.01), fontsize=8,
                         arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.5), ha='center')
    
        # Establecer las etiquetas del eje x solo para los armónicos con las magnitudes más grandes
        ax2.set_xticks([x for x, _ in top_harmonics])
        ax2.set_xticklabels([f"{x:.0f}" for x, _ in top_harmonics])
    
        fig.tight_layout()  # Ajustar el espacio entre los subplots
        fig.canvas.draw()  # Redibujar el gráfico


# Agregar un atributo global 'paused' para controlar la actualización de la gráfica
paused = False

# Función para alternar el estado de pausa
def toggle_pause():
    global paused
    paused = not paused
    if paused:
        pause_button.config(text="Reanudar")
        print("Gráfica pausada.")
    else:
        pause_button.config(text="Pausar")
        print("Gráfica reanudada.")


def create_tkinter_window():
    global pause_button
    # Crear la ventana de aplicación de Tkinter
    root = tk.Tk()
    root.title("Análisis de señales digitales")

    # Crear el objeto Figure y los ejes de Matplotlib
    fig = Figure(figsize=(8, 6), dpi=100)
    ax1 = fig.add_subplot(2, 2, 1)
    ax2 = fig.add_subplot(2, 2, 2)
    ax3 = fig.add_subplot(2, 2, 3)
    ax4 = fig.add_subplot(2, 2, 4)

    # Crear el canvas de Tkinter y agregarlo a la ventana
    canvas = FigureCanvasTkAgg(fig, master=root)
    canvas.draw()
    canvas.get_tk_widget().grid(row=0, column=0)

    pause_button = tk.Button(root, text="Pausar/Reanudar", command=toggle_pause)
    pause_button.grid(row=1, column=0)

    return root, fig, ax1, ax2, ax3, ax4, pause_button


FDVoltage_dict = {}
DATVoltage_dict = {}
PhaseVoltage = 0.0

def VoltageFFT(signal, sample_rate, i, fig, ax3, ax4):
    global PhaseVoltage, FDVoltage_dict, THD_dict, sincvoltaje1
    
    p = i
    N = len(signal)
    T = 1 / sample_rate

    # Restar la media de la señal para eliminar el componente de continua
    signal -= np.mean(signal)

    # Aplicar la ventana de Hamming a la señal
    windowed_signal = signal * np.hamming(N)

    # Calcular la FFT de la señal y las frecuencias correspondientes
    yf = np.fft.rfft(windowed_signal)
    xf = np.fft.rfftfreq(N, T)
    
    if sample_rate > 5100:
        # Interpolar la señal para obtener una mayor resolución en frecuencia
        f = interpolate.interp1d(xf, yf)
        xnew = np.arange(0, 50 * 51, 1)
        ynew = f(xnew)

        # Calcular las amplitudes de los armónicos
        amplitudes = 2.0 / N * np.abs(ynew)
        if not paused:
            plot_signal_and_fft(signal, xnew, amplitudes, p, fig, ax3, ax4,'Voltage',args.use_tkinter)
        harmonic_amplitudes = []
        complex_values = []
        
        # Iterar a través de los primeros 51 armónicos
        for idx in range(0, 50 * 51, 50):
            max_complex_value = max(ynew[idx:idx + 10])
            max_amplitude = max(amplitudes[idx:idx + 10])
            complex_values.append(max_complex_value)
            harmonic_amplitudes.append(max_amplitude)

        # Encontrar el índice del armónico fundamental
        fundamental_idx = np.argmax(harmonic_amplitudes)

        # Extraer la amplitud del armónico fundamental
        fundamental_amplitude = harmonic_amplitudes[fundamental_idx]

        # Calcular la fase del armónico fundamental
        PhaseVoltage = np.angle(complex_values[fundamental_idx])

        # Calcular el factor de distorsión de voltaje (FDVoltage)
        FDVoltage_dict[p] = fundamental_amplitude / np.sum(harmonic_amplitudes)

        # Calcular el Factor de Distorsión Armónica Total (THD)
        DATVoltage_dict[p] = np.sqrt((np.sum(harmonic_amplitudes) ** 2 - fundamental_amplitude ** 2) / (fundamental_amplitude ** 2))

        sincvoltaje1 = 1
        return fundamental_amplitude, PhaseVoltage, FDVoltage_dict[p], DATVoltage_dict[p], sincvoltaje1
   

CosPhi_dict = {}
FP_dict = {}
FDCurrent_dict = {}
DATCurrent_dict = {}
PhaseCurrent = 0.0
sincvoltaje1 = 0
desfaseCGE = {}
def CurrentFFT(signal, sample_rate, i, Irms, sincvoltaje1, PhaseVoltage, fundamental_amplitude_current, fig, ax1, ax2):
    global CosPhi_dict, FP_dict, THDCurrent_dict, FDCurrent_dict, PhaseCurrent,paused
    
    p = i
    N = len(signal)
    T = 1 / sample_rate
    print(f'N: {N}')
    # Restar la media de la señal para eliminar el componente de continua
    signal -= np.mean(signal)

    # Aplicar la ventana de Hamming a la señal
    windowed_signal = signal * np.hamming(N)

    # Calcular la FFT de la señal y las frecuencias correspondientes
    yf = np.fft.rfft(windowed_signal)
    xf = np.fft.rfftfreq(N, T)

    # Interpolar la señal para obtener una mayor resolución en frecuencia
    f = interpolate.interp1d(xf, yf)
    xnew = np.arange(0, 50 * 51, 1)
    ynew = f(xnew)
    # Calcular las amplitudes de los armónicos
    amplitudes = 2.0 / N * np.abs(ynew)
    if not paused:
        plot_signal_and_fft(signal, xnew, amplitudes, p, fig, ax1, ax2,"Current",args.use_tkinter)
    
    harmonic_amplitudes = []
    complex_values = []

    # Iterar a través de los primeros 51 armónicos
    for idx in range(0, 50 * 51, 50):
        max_complex_value = max(ynew[idx:idx + 10])
        max_amplitude = max(amplitudes[idx:idx + 10])
        complex_values.append(max_complex_value)
        harmonic_amplitudes.append(max_amplitude)

    # Calcular la suma de las amplitudes de los armónicos
    total_amplitude = np.sum(harmonic_amplitudes)

    # Extraer la amplitud del armónico fundamental
    fundamental_amplitude = harmonic_amplitudes[0]

    # Calcular la fase del armónico fundamental
    PhaseCurrent = np.angle(complex_values[0])

    # Calcular el factor de distorsión de corriente (FDCurrent)
    FDCurrent_dict[p] = fundamental_amplitude / total_amplitude

    # Calcular el Factor de Distorsión Armónica Total (THD)
    DATCurrent_dict[p] = np.sqrt((total_amplitude ** 2 - fundamental_amplitude ** 2) / (fundamental_amplitude ** 2))

    # Calcular la proporción de corriente en función de la amplitud fundamental del voltaje
    Irms_ratio = Irms / (np.sqrt(fundamental_amplitude_current ** 2 + fundamental_amplitude ** 2))

    if sincvoltaje1 == 1:
        if PhaseVoltage - PhaseCurrent >= 0:
            desfaseCGE[p] = "Corriente Adelantada a Voltaje"
        else:
            desfaseCGE[p] = "Voltaje Adelantado a Corriente"
        FP_dict[p] = np.cos(PhaseVoltage - PhaseCurrent) * FDCurrent_dict[p]
        CosPhi_dict[p] = np.cos(PhaseVoltage - PhaseCurrent)

    return FP_dict[p],CosPhi_dict[p],desfaseCGE[p], DATCurrent_dict[p], FDCurrent_dict[p]


    

energy_times = [datetime.datetime.now()] * largo_lista 
energies = [0.0] * largo_lista
one_hour_energies = [0.0] * largo_lista
aparent_power = 0.0
active_power = 0.0
reactive_power = 0.0
access_hour_energy = 0
aparent_power_list = []
current_load_list = []
current_panels_list = []
aparent_power_panels_list = []
sum_power_load = 0
sum_power_panels = 0
sum_power_grid = 0
count_power = 0
count_power2 = 0
count_current2 = 0
count_current = 0
currents_load = 0
currents_panels = 0
energy_factor = 2.5
EnergyFactor=2.5
# Use dictionaries to store variable values
TimeA = {}
TimeB = {}
Energy = {}
OneHourEnergy = {}
AparentPower_dict = {}
ActivePower_dict = {}
ReactivePower_dict = {}
FP_dict = {}
CosPhi_dict = {}
FDVoltage_dict = {}
FDCurrent_dict = {}
DATVoltage_dict = {}
DATCurrent_dict = {}
TimeInit = datetime.datetime.now()
print(f'Hora Inicio: {TimeInit}')

TimeA = {i: datetime.datetime.now() for i in range(1, largo_lista + 1)}

Vrms_lists = {i: [] for i in range(1, largo_lista + 1)}
Irms_lists = {i: [] for i in range(1, largo_lista + 1)}
FP_Reactive_lists = {i: [] for i in range(1, largo_lista + 1)}
FP_Inductive_lists = {i: [] for i in range(1, largo_lista + 1)}
FDVoltage_lists = {i: [] for i in range(1, largo_lista + 1)}
FDCurrent_lists = {i: [] for i in range(1, largo_lista + 1)}
DATVoltage_lists = {i: [] for i in range(1, largo_lista + 1)}
DATCurrent_lists = {i: [] for i in range(1, largo_lista + 1)}
OneHourEnergy_lists = {i: [] for i in range(1, largo_lista + 1)} 
Energy_lists = {i: [] for i in range(1, largo_lista + 1)}
ActivePower_lists = {i: [] for i in range(1, largo_lista + 1)}
ReactivePower_lists = {i: [] for i in range(1, largo_lista + 1)}
AparentPower_lists = {i: [] for i in range(1, largo_lista + 1)}

print(f' Vrms_lists: {Vrms_lists}')

def Potencias(i, Irms, Vrms, potrmsCGE, FP, CosPhi,desfaseCGE, DATCurrent, FDCurrent):
    global Vrms_lists, Irms_lists, called_this_minute
    global ActivePower_lists, ReactivePower_lists, AparentPower_lists, OneHourEnergy_lists, Energy_lists
    global FP_Reactive_lists, FP_Inductive_lists, FDVoltage_lists, FDCurrent_lists, DATVoltage_lists, DATCurrent_lists
    
    AparentPower = Vrms*Irms               
    if(potrmsCGE>=0):
          ActivePower = Vrms*Irms*CosPhi
          ActivePower = np.abs(ActivePower)
    else:
          ActivePower = Vrms*Irms*CosPhi
          ActivePower = np.abs(ActivePower)
          ActivePower = ActivePower*(-1)
    ReactivePower = Vrms*Irms*np.sin(PhaseVoltage-PhaseCurrent)
  
    #print(f"  TimeA: {TimeA[i]}")
    TimeB[i] = datetime.datetime.now()
    delta_readable = (TimeB[i] - TimeA[i]).total_seconds()
    delta = (((TimeB[i] - TimeA[i]).microseconds) / 1000 + ((TimeB[i] - TimeA[i]).seconds) * 1000) / 10000000000
    Energy[i] = Energy.get(i, 0) + np.abs(AparentPower * delta * EnergyFactor)
    OneHourEnergy[i] = OneHourEnergy.get(i, 0) + np.abs(AparentPower * delta * EnergyFactor)
    TimeA[i] = datetime.datetime.now()

    #print(f"Iteration {i}:")
    #print(f"  Vrms: {Vrms}")
    #print(f"  AparentPower: {AparentPower}")
    #print(f"  ActivePower: {ActivePower}")
    #print(f"  ReactivePower: {ReactivePower}")
    #print(f"  Energy: {Energy[i]}")
    #print(f"  OneHourEnergy: {OneHourEnergy[i]}")
    #print(f"  TimeB: {TimeB[i]}")
    #print(f"  TimeA: {TimeA[i]}")
    #print(f"  Delta: {delta_readable} seconds\n")
#
    print(f"  Vrms_lists: {Vrms_lists}")
    Vrms_lists[i].append(Vrms)
    Irms_lists[i].append(Irms)
    ActivePower_lists[i].append(ActivePower)
    ReactivePower_lists[i].append(ReactivePower)
    AparentPower_lists[i].append(AparentPower)
    if FP > 0.0:
        FP_Reactive_lists[i].append(FP_dict[i])
    else:
        FP_Inductive_lists[i].append(FP_dict[i])
    FDVoltage_lists[i].append(FDVoltage_dict[i])
    FDCurrent_lists[i].append(FDCurrent_dict[i])
    DATVoltage_lists[i].append(DATVoltage_dict[i])
    DATCurrent_lists[i].append(DATCurrent_dict[i])
    OneHourEnergy_lists[i].append(OneHourEnergy[i])
    Energy_lists[i].append(Energy[i])
    #print(f'{Vrms_lists[i]} .... {i}')
    #if len(Vrms_lists[i]) == 5:
    now = datetime.datetime.now()
    if now.minute % 5 == 0:
        if not called_this_minute:
            call_every_five_minutes()
            called_this_minute = True
    else:
        called_this_minute = False
        
        # Limpiar las listas después de llamar a Maximo15min
        

    #SaveDataCsv(Vrms, Irms, ActivePower_dict[i], ReactivePower_dict[i], AparentPower_dict[i], FP_dict[i], CosPhi_dict[i], FDVoltage_dict[i], FDCurrent_dict[i], DATVoltage_dict[i], DATCurrent_dict[i], Energy[i], OneHourEnergy[i], i, k1, f1)

called_this_minute = False
def call_every_five_minutes():
    print(f"entrando a call_every_five_minutes {Variable_list}")
    Variable=None
    for i in range(1, largo_lista+1):
        print(f"entrando a call_every_five_minutes {Variable_list[i-1]}")
        t = threading.Thread(target=Maximo15min, args=(data,userId,dId,Variable_list[i-1],Vrms_lists[i], Irms_lists[i], ActivePower_lists[i], ReactivePower_lists[i], AparentPower_lists[i], FP_Reactive_lists[i], FP_Inductive_lists[i], FDVoltage_lists[i], FDCurrent_lists[i], DATVoltage_lists[i], DATCurrent_lists[i], OneHourEnergy_lists[i], Energy_lists[i], mongo_connect))
        t.start()
        t.join()
        Vrms_lists[i] = []
        Irms_lists[i] = []
        ActivePower_lists[i] = []
        ReactivePower_lists[i] = []
        AparentPower_lists[i] = []
        FP_Reactive_lists[i] = []
        FP_Inductive_lists[i] = []
        FDVoltage_lists[i] = []
        FDCurrent_lists[i] = []
        DATVoltage_lists[i] = []
        DATCurrent_lists[i] = []
        OneHourEnergy_lists[i] = []
        Energy_lists[i] = []





buffer_voltage = {i: [] for i in range(1, largo_lista + 1)}
buffer_current = {i: [] for i in range(1, largo_lista + 1)}
print(f' buffer_voltage: {buffer_voltage}')
def generate_simulated_data():
    signal_type = random.choice(range(11, 11 + largo_lista))
    duration = 0.14
    sampling_rate = 30000  # 30 KS/s
    time = np.linspace(0, duration, int(duration * sampling_rate))

    # Ejemplo de señal de voltaje: seno de 60 Hz
    voltage_signal = 220 * np.sqrt(2) * np.sin(2 * np.pi * 60 * time)
    
    # Número de armónicos aleatorios
    num_harmonics = random.randint(1, 5)

    for _ in range(num_harmonics):
        # Frecuencia y amplitud aleatoria para cada armónico
        freq = random.uniform(120, 1000)
        amp = random.uniform(0.05, 0.2) * 220 * np.sqrt(2)
        
        # Agregar armónico a la señal de voltaje
        voltage_signal += amp * np.sin(2 * np.pi * freq * time)

    # Ejemplo de señal de corriente: seno de 60 Hz desfasado 45 grados
    current_signal = 1 * np.sqrt(2) * np.sin(2 * np.pi * 60 * time + np.pi / 4)

    for _ in range(num_harmonics):
        # Frecuencia y amplitud aleatoria para cada armónico
        freq = random.uniform(120, 1000)
        amp = random.uniform(0.05, 0.2) * 1 * np.sqrt(2)

        # Agregar armónico a la señal de corriente
        current_signal += amp * np.sin(2 * np.pi * freq * time + np.pi / 4)

    samplings = 30000  # Suponiendo que hay 30000 muestreos por segundo en el sistema

    simulated_data = [signal_type] + voltage_signal.tolist() + current_signal.tolist() + [samplings]
    return ','.join(map(str, simulated_data))

def process_signal(list_voltage, list_current, samplings, i, fig, ax1, ax2, ax3, ax4):
    global buffer_voltage, buffer_current

    list_voltage_filtered = butter_lowpass_filter(list_voltage, 2500, samplings)
    list_current_filtered = butter_lowpass_filter(list_current, 2500, samplings)

    list_final_voltage = list_voltage_filtered[104:4200]
    list_final_current = list_current_filtered[104:4200]

    max_voltage = np.min(get_max_values(list_final_voltage, 100))
    min_voltage = np.max(get_min_values(list_final_voltage, 100))
    dc_voltage_median = (max_voltage + min_voltage) / 2
    no_voltage_offset = (list_final_voltage - dc_voltage_median)
    vrms = VoltajeRms(no_voltage_offset) * 0.92
    print(vrms)
    print(i)
    buffer_voltage[i].append(vrms)
    if len(buffer_voltage[i]) >= 3:
        median_buffer_voltage = np.median(buffer_voltage[i])
        vrms = max(min(median_buffer_voltage * 0.71, 240), 0) if median_buffer_voltage >= 13 else 0
        print(f'Vrms {i}: {vrms}')
        fundamental_amplitude_voltage, PhaseVoltage, FDVoltage_dict,THD_dict,sincvoltaje1 = VoltageFFT(no_voltage_offset, samplings, i, fig, ax3, ax4)

        buffer_voltage[i] = []

    max_current = np.median(get_max_values(list_final_current, 50))
    min_current = np.median(get_min_values(list_final_current, 50))
    dc_current_median = (max_current + min_current) / 2
    no_current_offset = list_final_current - dc_current_median
    irms = CorrienteRms(no_current_offset)

    buffer_current[i].append(irms)
    if len(buffer_current[i]) >= 3 and vrms < 240:
        median_buffer_current = np.median(buffer_current[i])

        if median_buffer_current > 430:
            Irms = median_buffer_current * 0.0133
        else:
            Irms = (0.0046 + 0.000282 * median_buffer_current - 0.00000328 * (median_buffer_current ** 2) +
                    0.0000000167 * (median_buffer_current ** 3) - 0.0000000000382 * (median_buffer_current ** 4) +
                    0.0000000000000322 * (median_buffer_current ** 5)) * median_buffer_current

        #irms *= 0.94
        print(f'Irms {i}: {irms}')
        FP, CosPhi,desfaseCGE, DATCurrent, FDCurrent = CurrentFFT(no_current_offset, samplings, i, Irms, sincvoltaje1, PhaseVoltage, fundamental_amplitude_voltage, fig, ax1, ax2)
        potrmsCGE = PotenciaRms(no_current_offset, no_voltage_offset)
        print(f'potrmsCGE {i}: {potrmsCGE}')
        Potencias(i, irms, vrms, potrmsCGE, FP, CosPhi,desfaseCGE, DATCurrent, FDCurrent)
        
        buffer_current[i] = []



def receive(use_serial,root=None, fig=None, ax1=None, ax2=None, ax3=None, ax4=None):
    print(f'use_serial receive: {use_serial}')
    time.sleep(10)
    while True:
        try:
            if use_serial:
                esp32_bytes = esp32.readline()
                decoded_bytes = str(esp32_bytes[0:len(esp32_bytes)-2].decode("utf-8"))
            else:
                decoded_bytes = generate_simulated_data().encode('utf-8')
        except:
            print("Error en la codificación")
            continue

        np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')
        time.sleep(2)

        if len(np_array) == 8402:
            signal_type_to_index = {
                11: 1,
                22: 2,
                33: 3,
                44: 4,
                55: 5,
                66: 6,
                77: 7,
                88: 8,
                99: 9
            }
            print_memory_usage()
            signal_type = np_array[0]
            if signal_type in signal_type_to_index:
                i = signal_type_to_index[signal_type]
                samplings = np_array[-1]

                list_voltage = np_array[1:4201]
                list_current = np_array[4201:8401]
                
                process_signal(list_voltage, list_current, samplings, i, fig, ax1, ax2, ax3, ax4)
                if root is not None:
                    root.after(100, receive,use_serial, root, fig, ax1, ax2, ax3, ax4)
                break


def process_signals_without_tkinter(use_serial,root=None, fig=None, ax1=None, ax2=None, ax3=None, ax4=None):
    if use_serial:
        try:                           
            esp32 = serial.Serial('/dev/ttyUSB0', 230400, timeout=0.5)
            esp32.flushInput()                          
                 
        except:
            esp32 = serial.Serial('/dev/ttyUSB1', 230400, timeout=0.5)
            esp32.flushInput()
    
    while True:
        try:
            if use_serial:
                esp32_bytes = esp32.readline()
                decoded_bytes = str(esp32_bytes[0:len(esp32_bytes)-2].decode("utf-8"))
            else:
                decoded_bytes = generate_simulated_data().encode('utf-8')
        except:
            print("Error en la codificación")
            continue

        np_array = np.fromstring(decoded_bytes, dtype=float, sep=',')
        time.sleep(2)

        signal_type_to_index = {key: index for index, key in enumerate(range(11, 11 + largo_lista), start=1)}
            #print_memory_usage()

        signal_type = np_array[0]
        if signal_type in signal_type_to_index:
            i = signal_type_to_index[signal_type]
            samplings = np_array[-1]
            list_voltage = np_array[1:4201]
            list_current = np_array[4201:8401]
            process_signal(list_voltage, list_current, samplings, i, fig, ax1, ax2, ax3, ax4)
            #break

mongo_connect=0
data=None
def main(use_tkinter, use_serial, use_mqtt, use_mongo):
    global mongo_connect
    print(f'use_tkinter: {use_tkinter}')
    print(f'use_serial: {use_serial}')
    time.sleep(10)
    if use_mqtt:
        global data
        _,data = get_mqtt_credentials()
        print("Conectando al broker MQTT ..")
        ConnectToBroker()
    if use_mongo:
        print("Conectando a MongoDB ..")
        mongo_connect=1
    if use_tkinter:
        print("Usando tkinter")
        # Crear la ventana de aplicación de Tkinter y los objetos de Matplotlib
        root, fig, ax1, ax2, ax3, ax4, pause_button = create_tkinter_window()

        # Llamar a root.mainloop() para iniciar la interfaz gráfica
        root.after(100, receive, use_serial, root, fig, ax1, ax2, ax3, ax4)
        root.mainloop()
    else:
        # Ejecutar el código sin la interfaz gráfica
        process_signals_without_tkinter(use_serial)
    

if __name__ == "__main__":
   
    
    if args.get_devices:
           #mqtt_credentials = get_mqtt_credentials()
           get_mqtt_credentials()
           #print(mqtt_credentials)
           #if mqtt_credentials is not None:
           #    ConnectToBroker(mqtt_credentials)
           sys.exit()
    main(args.use_tkinter, args.use_serial, args.use_mqtt, args.use_mongo)