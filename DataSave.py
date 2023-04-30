import pymongo
from pymongo import MongoClient
import numpy as np
from data_db import list_data_db_insert


def Maximo15min(Variable, Vrms, Irms, ActivePower, ReactivePower, AparentPower, FP_Reactive_lists, FP_Inductive_lists, FDVoltage, FDCurrent, DATVoltage, DATCurrent, OneHourEnergy, Energy, mongo_connect):
       try:
            max_Vrms = max(Vrms)
            min_Vrms = min(Vrms)
            avg_Vrms = sum(Vrms) / len(Vrms)
        
            max_Irms = max(Irms)
            min_Irms = min(Irms)
            avg_Irms = sum(Irms) / len(Irms)
        
            max_ActivePower = max(ActivePower)
            min_ActivePower = min(ActivePower)
            avg_ActivePower = sum(ActivePower) / len(ActivePower)
        
            max_ReactivePower = max(ReactivePower)
            min_ReactivePower = min(ReactivePower)
            avg_ReactivePower = sum(ReactivePower) / len(ReactivePower)
        
            max_AparentPower = max(AparentPower)
            min_AparentPower = min(AparentPower)
            avg_AparentPower = sum(AparentPower) / len(AparentPower)
            
            if(len(FP_Inductive_lists)>0):
                   Max_FPInductive=max(FP_Inductive_lists)
                   Avg_FPInductive=sum(FP_Inductive_lists) / len(FP_Inductive_lists)
                   Min_FPInductive=min(FP_Inductive_lists)
            else:
                   Max_FPInductive=-0.59
                   Avg_FPInductive=-0.99
                   Min_FPInductive=-0.99
            if(len(FP_Reactive_lists)>0):
                   Max_FPReactive=max(FP_Reactive_lists)
                   Avg_FPReactive=sum(FP_Reactive_lists) / len(FP_Reactive_lists)
                   Min_FPReactive=min(FP_Reactive_lists)
            else:
                   Max_FPReactive=0.99
                   Avg_FPReactive=0.99
                   Min_FPReactive=0.99
        
        
            max_FDVoltage = max(FDVoltage)
            min_FDVoltage = min(FDVoltage)
            avg_FDVoltage = sum(FDVoltage) / len(FDVoltage)
        
            max_FDCurrent = max(FDCurrent)
            min_FDCurrent = min(FDCurrent)
            avg_FDCurrent = sum(FDCurrent) / len(FDCurrent)
        
            max_DATVoltage = max(DATVoltage)
            min_DATVoltage = min(DATVoltage)
            avg_DATVoltage = sum(DATVoltage) / len(DATVoltage)
        
            max_DATCurrent = max(DATCurrent)
            min_DATCurrent = min(DATCurrent)
            avg_DATCurrent = sum(DATCurrent) / len(DATCurrent)
        
            max_OneHourEnergy = max(OneHourEnergy)
            min_OneHourEnergy = min(OneHourEnergy)
            avg_OneHourEnergy = sum(OneHourEnergy) / len(OneHourEnergy)
        
            max_Energy = max(Energy)
            min_Energy = min(Energy)
            avg_Energy = sum(Energy) / len(Energy)
        
            document = {
                'max_Vrms': max_Vrms,
                'min_Vrms': min_Vrms,
                'avg_Vrms': avg_Vrms,
                'max_Irms': max_Irms,
                'min_Irms': min_Irms,
                'avg_Irms': avg_Irms,
                'max_ActivePower': max_ActivePower,
                'min_ActivePower': min_ActivePower,
                'avg_ActivePower': avg_ActivePower,
                'max_ReactivePower': max_ReactivePower,
                'min_ReactivePower': min_ReactivePower,
                'avg_ReactivePower': avg_ReactivePower,
                'max_AparentPower': max_AparentPower,
                'min_AparentPower': min_AparentPower,
                'avg_AparentPower': avg_AparentPower,
                'Max_FPInductive': Max_FPInductive,
                'Min_FPInductive': Min_FPInductive,
                'Avg_FPInductive': Avg_FPInductive,
                'Max_FPReactive': Max_FPReactive,
                'Min_FPReactive': Min_FPReactive,
                'Avg_FPReactive': Avg_FPReactive,
                'max_FDVoltage': max_FDVoltage,
                'min_FDVoltage': min_FDVoltage,
                'avg_FDVoltage': avg_FDVoltage,
                'max_FDCurrent': max_FDCurrent,
                'min_FDCurrent': min_FDCurrent,
                'avg_FDCurrent': avg_FDCurrent,
                'max_DATVoltage': max_DATVoltage,
                'min_DATVoltage': min_DATVoltage,
                'avg_DATVoltage': avg_DATVoltage,
                'max_DATCurrent': max_DATCurrent,
                'min_DATCurrent': min_DATCurrent,
                'avg_DATCurrent': avg_DATCurrent,
                'max_OneHourEnergy': max_OneHourEnergy,
                'min_OneHourEnergy': min_OneHourEnergy,
                'avg_OneHourEnergy': avg_OneHourEnergy,
                'max_Energy': max_Energy,
                'min_Energy': min_Energy,
                'avg_Energy': avg_Energy,
            }
            print(document)
            if mongo_connect == 1:
               list_data_db_insert(document)
            #try:
    #   return (max_Vrms, min_Vrms, avg_Vrms, max_Irms, min_Irms, avg_Irms, max_ActivePower, min_ActivePower, avg_ActivePower, 
    #       max_ReactivePower, min_ReactivePower, avg_ReactivePower, max_AparentPower, min_AparentPower, avg_AparentPower, Max_FPInductive, 
    #       Avg_FPInductive, Min_FPInductive, Max_FPReactive, Avg_FPReactive, Min_FPReactive, max_FDVoltage, min_FDVoltage, avg_FDVoltage, 
    #       max_FDCurrent, min_FDCurrent, avg_FDCurrent, max_DATVoltage, min_DATVoltage, avg_DATVoltage, max_DATCurrent, min_DATCurrent, 
    #       avg_DATCurrent, max_OneHourEnergy, min_OneHourEnergy, avg_OneHourEnergy, max_Energy, min_Energy, avg_Energy)
       except Exception as e:
         print("Ocurrió un error al calcular las estadísticas:", e)
         #return None