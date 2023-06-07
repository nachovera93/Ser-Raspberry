import pymongo
from pymongo import MongoClient
import numpy as np
from connector import iot_ser_db
from connector import local_db
import json
from mqtt_helper import userId, dId, SendDataToBroker,client
from datetime import datetime
import time

#db = iot_ser_db()
#from data_db import list_data_db_insert


def insert_data(collection_name, data):
    try:
        db = iot_ser_db()
        col_use = db[collection_name]
        col_use.insert_one(data)
    except Exception as e:
        print(f"Excepción al insertar datos en Mongo: {str(e)}")

def Maximo15min(data,userId, dId, Variable_list, Vrms, Irms, ActivePower, ReactivePower, AparentPower, FP_Reactive_lists, FP_Inductive_lists, FDVoltage, FDCurrent, DATVoltage, DATCurrent, OneHourEnergy, Energy, mongo_connect):
       #try:
            print("Maximo15min ")
            print(f'Mongoconnect: {mongo_connect}')
            max_Vrms = round(max(Vrms), 2)
            min_Vrms = round(min(Vrms), 2)
            avg_Vrms = round(sum(Vrms) / len(Vrms), 2)
            
            max_Irms = round(max(Irms), 2)
            min_Irms = round(min(Irms), 2)
            avg_Irms = round(sum(Irms) / len(Irms), 2)
            
            max_ActivePower = round(max(ActivePower), 2)
            min_ActivePower = round(min(ActivePower), 2)
            avg_ActivePower = round(sum(ActivePower) / len(ActivePower), 2)
            
            max_ReactivePower = round(max(ReactivePower), 2)
            min_ReactivePower = round(min(ReactivePower), 2)
            avg_ReactivePower = round(sum(ReactivePower) / len(ReactivePower), 2)
            
            max_AparentPower = round(max(AparentPower), 2)
            min_AparentPower = round(min(AparentPower), 2)
            avg_AparentPower = round(sum(AparentPower) / len(AparentPower), 2)
            
            if(len(FP_Inductive_lists)>0):
                Max_FPInductive=round(max(FP_Inductive_lists), 2)
                Avg_FPInductive=round(sum(FP_Inductive_lists) / len(FP_Inductive_lists), 2)
                Min_FPInductive=round(min(FP_Inductive_lists), 2)
            else:
                Max_FPInductive=-0.59
                Avg_FPInductive=-0.99
                Min_FPInductive=-0.99
            
            if(len(FP_Reactive_lists)>0):
                Max_FPReactive=round(max(FP_Reactive_lists), 2)
                Avg_FPReactive=round(sum(FP_Reactive_lists) / len(FP_Reactive_lists), 2)
                Min_FPReactive=round(min(FP_Reactive_lists), 2)
            else:
                Max_FPReactive=0.99
                Avg_FPReactive=0.99
                Min_FPReactive=0.99
            
            max_FDVoltage = round(max(FDVoltage), 2)
            min_FDVoltage = round(min(FDVoltage), 2)
            avg_FDVoltage = round(sum(FDVoltage) / len(FDVoltage), 2)
            
            max_FDCurrent = round(max(FDCurrent), 2)
            min_FDCurrent = round(min(FDCurrent), 2)
            avg_FDCurrent = round(sum(FDCurrent) / len(FDCurrent), 2)
            
            max_DATVoltage = round(max(DATVoltage), 2)
            min_DATVoltage = round(min(DATVoltage), 2)
            avg_DATVoltage = round(sum(DATVoltage) / len(DATVoltage), 2)
            
            max_DATCurrent = round(max(DATCurrent), 2)
            min_DATCurrent = round(min(DATCurrent), 2)
            avg_DATCurrent = round(sum(DATCurrent) / len(DATCurrent), 2)
            
            max_OneHourEnergy = round(max(OneHourEnergy), 2)
            min_OneHourEnergy = round(min(OneHourEnergy), 2)
            avg_OneHourEnergy = round(sum(OneHourEnergy) / len(OneHourEnergy), 2)
            
            max_Energy = round(max(Energy), 2)
            min_Energy = round(min(Energy), 2)
            avg_Energy = round(sum(Energy) / len(Energy), 2)
        
            if mongo_connect == 1:
                timestamp = datetime.now()

                #for variable in Variable_list:
                
                    # Creamos las variables con nombres dinámicos
                max_vrms_name = "max_vrms_{}".format(Variable_list)
                min_vrms_name = "min_vrms_{}".format(Variable_list)
                avg_vrms_name = "avg_vrms_{}".format(Variable_list)
                
                max_irms_name = "max_irms_{}".format(Variable_list)
                min_irms_name = "min_irms_{}".format(Variable_list)
                avg_irms_name = "avg_irms_{}".format(Variable_list)
                
                max_active_power_name = "max_active_power_{}".format(Variable_list)
                min_active_power_name = "min_active_power_{}".format(Variable_list)
                avg_active_power_name = "avg_active_power_{}".format(Variable_list)
                
                max_reactive_power_name = "max_reactive_power_{}".format(Variable_list)
                min_reactive_power_name = "min_reactive_power_{}".format(Variable_list)
                avg_reactive_power_name = "avg_reactive_power_{}".format(Variable_list)
                
                max_apparent_power_name = "max_apparent_power_{}".format(Variable_list)
                min_apparent_power_name = "min_apparent_power_{}".format(Variable_list)
                avg_apparent_power_name = "avg_apparent_power_{}".format(Variable_list)
                
                max_fp_inductive_name = "max_fp_inductive_{}".format(Variable_list)
                min_fp_inductive_name = "min_fp_inductive_{}".format(Variable_list)
                avg_fp_inductive_name = "avg_fp_inductive_{}".format(Variable_list)
                
                max_fp_reactive_name = "max_fp_reactive_{}".format(Variable_list)
                min_fp_reactive_name = "min_fp_reactive_{}".format(Variable_list)
                avg_fp_reactive_name = "avg_fp_reactive_{}".format(Variable_list)
                
                max_fd_voltage_name = "max_fd_voltage_{}".format(Variable_list)
                min_fd_voltage_name = "min_fd_voltage_{}".format(Variable_list)
                avg_fd_voltage_name = "avg_fd_voltage_{}".format(Variable_list)
                
                max_fd_current_name = "max_fd_current_{}".format(Variable_list)
                min_fd_current_name = "min_fd_current_{}".format(Variable_list)
                avg_fd_current_name = "avg_fd_current_{}".format(Variable_list)
                
                max_dat_voltage_name = "max_dat_voltage_{}".format(Variable_list)
                min_dat_voltage_name = "min_dat_voltage_{}".format(Variable_list)
                avg_dat_voltage_name = "avg_dat_voltage_{}".format(Variable_list)
                
                max_dat_current_name = "max_dat_current_{}".format(Variable_list)
                min_dat_current_name = "min_dat_current_{}".format(Variable_list)
                avg_dat_current_name = "avg_dat_current_{}".format(Variable_list)
                
                max_one_hour_energy_name = "max_one_hour_energy_{}".format(Variable_list)
                min_one_hour_energy_name = "min_one_hour_energy_{}".format(Variable_list)
                avg_one_hour_energy_name = "avg_one_hour_energy_{}".format(Variable_list)
                max_energy_name = "max_energy_{}".format(Variable_list)
                min_energy_name = "min_energy_{}".format(Variable_list)
                avg_energy_name = "avg_energy_{}".format(Variable_list)
                # Peparamos los documentos para insertar en MongoDB
                voltage_document = {
                    'userId': userId,
                    'dId': dId,
                    "variable": "Yb7TcLx9pK",
                    max_vrms_name: max_Vrms,
                    min_vrms_name: min_Vrms,
                    avg_vrms_name: avg_Vrms,
                    max_fd_voltage_name: max_FDVoltage,
                    min_fd_voltage_name: min_FDVoltage,
                    avg_fd_voltage_name: avg_FDVoltage,
                    max_dat_voltage_name: max_DATVoltage,
                    min_dat_voltage_name: min_DATVoltage,
                    avg_dat_voltage_name: avg_DATVoltage,
                    'timestamp': timestamp,
                }
                insert_data("VoltajeData", voltage_document)
                
                current_document = {
                    'userId': userId,
                    'dId': dId,
                    "variable": "Qf4GjSd2hN",
                    max_irms_name: max_Irms,
                    min_irms_name: min_Irms,
                    avg_irms_name: avg_Irms,
                    max_fd_current_name: max_FDCurrent,
                    min_fd_current_name: min_FDCurrent,
                    avg_fd_current_name: avg_FDCurrent,
                    max_dat_current_name: max_DATCurrent,
                    min_dat_current_name: min_DATCurrent,
                    avg_dat_current_name: avg_DATCurrent,
                    'timestamp': timestamp,
                }
                insert_data("CorrienteData", current_document)
                
                power_document = {
                    'userId': userId,
                    'dId': dId,
                    "variable": "Lm6VzKx8rJ",
                    max_active_power_name: max_ActivePower,
                    min_active_power_name: min_ActivePower,
                    avg_active_power_name: avg_ActivePower,
                    max_reactive_power_name: max_ReactivePower,
                    min_reactive_power_name: min_ReactivePower,
                    avg_reactive_power_name: avg_ReactivePower,
                    max_apparent_power_name: max_AparentPower,
                    min_apparent_power_name: min_AparentPower,
                    avg_apparent_power_name: avg_AparentPower,
                    max_fp_inductive_name: Max_FPInductive,
                    min_fp_inductive_name: Min_FPInductive,
                    avg_fp_inductive_name: Avg_FPInductive,
                    max_fp_reactive_name: Max_FPReactive,
                    min_fp_reactive_name: Min_FPReactive,
                    avg_fp_reactive_name: Avg_FPReactive,
                    'timestamp': timestamp,
                }
                insert_data("PotenciaData", power_document)
                
                energy_document = {
                    'userId': userId,
                    'dId': dId,
                    "variable": "Hc2BtFg9nR",
                    max_one_hour_energy_name: max_OneHourEnergy,
                    min_one_hour_energy_name: min_OneHourEnergy,
                    avg_one_hour_energy_name: avg_OneHourEnergy,
                    max_energy_name: max_Energy,
                    min_energy_name: min_Energy,
                    avg_energy_name: avg_Energy,
                    'timestamp': timestamp,
                }
                insert_data("EnergiaData", energy_document)
                print("Valores Insertados")
                
                variableToCollection = {
                  "VoltajeData": {
                    "variable": 'Yb7TcLx9pK',
                    "values": {
                      max_vrms_name: max_Vrms,
                      min_vrms_name: min_Vrms,
                      avg_vrms_name: avg_Vrms,
                      max_fd_voltage_name: max_FDVoltage,
                      min_fd_voltage_name: min_FDVoltage,
                      avg_fd_voltage_name: avg_FDVoltage,
                      max_dat_voltage_name: max_DATVoltage,
                      min_dat_voltage_name: min_DATVoltage,
                      avg_dat_voltage_name: avg_DATVoltage,
                    }
                  },
                  "CorrienteData": {
                    "variable": 'Qf4GjSd2hN',
                    "values": {
                      max_irms_name: max_Irms,
                      min_irms_name: min_Irms,
                      avg_irms_name: avg_Irms,
                      max_fd_current_name: max_FDCurrent,
                      min_fd_current_name: min_FDCurrent,
                      avg_fd_current_name: avg_FDCurrent,
                      max_dat_current_name: max_DATCurrent,
                      min_dat_current_name: min_DATCurrent,
                      avg_dat_current_name: avg_DATCurrent,
                    }
                  },
                  "PotenciaData": {
                    "variable": 'Lm6VzKx8rJ',
                    "values": {
                      max_active_power_name: max_ActivePower,
                      min_active_power_name: min_ActivePower,
                      avg_active_power_name: avg_ActivePower,
                      max_reactive_power_name: max_ReactivePower,
                      min_reactive_power_name: min_ReactivePower,
                      avg_reactive_power_name: avg_ReactivePower,
                      max_apparent_power_name: max_AparentPower,
                      min_apparent_power_name: min_AparentPower,
                      avg_apparent_power_name: avg_AparentPower,
                      max_fp_inductive_name: Max_FPInductive,
                      min_fp_inductive_name: Min_FPInductive,
                      avg_fp_inductive_name: Avg_FPInductive,
                      max_fp_reactive_name: Max_FPReactive,
                      min_fp_reactive_name: Min_FPReactive,
                      avg_fp_reactive_name: Avg_FPReactive,
                    }
                  },
                  "EnergiaData": {
                    "variable": 'Hc2BtFg9nR',
                    "values": {
                      max_one_hour_energy_name: max_OneHourEnergy,
                      min_one_hour_energy_name: min_OneHourEnergy,
                      avg_one_hour_energy_name: avg_OneHourEnergy,
                      max_energy_name: max_Energy,
                      min_energy_name: min_Energy,
                      avg_energy_name: avg_Energy,
                    }
                  }
                };
                #print(variableToCollection)
                for categoryName, category in variableToCollection.items():
                    SendDataToBroker( time.time(), data, category["variable"], categoryName, **category["values"])


                
# Enviamos los va        lores por MQTT
        
        

    #   return (max_Vrms, min_Vrms, avg_Vrms, max_Irms, min_Irms, avg_Irms, max_ActivePower, min_ActivePower, avg_ActivePower, 
    #       max_ReactivePower, min_ReactivePower, avg_ReactivePower, max_AparentPower, min_AparentPower, avg_AparentPower, Max_FPInductive, 
    #       Avg_FPInductive, Min_FPInductive, Max_FPReactive, Avg_FPReactive, Min_FPReactive, max_FDVoltage, min_FDVoltage, avg_FDVoltage, 
    #       max_FDCurrent, min_FDCurrent, avg_FDCurrent, max_DATVoltage, min_DATVoltage, avg_DATVoltage, max_DATCurrent, min_DATCurrent, 
    #       avg_DATCurrent, max_OneHourEnergy, min_OneHourEnergy, avg_OneHourEnergy, max_Energy, min_Energy, avg_Energy)
       #except Exception as e:
       #  print("Ocurrió un error al calcular las estadísticas:", e)
         #return None