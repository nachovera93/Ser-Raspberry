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

def Maximo15min_1(Vrms,Irms,ActivePower,ReactivePower,AparentPower,FP,FDVoltage,FDCurrent,DATVoltage,DATCurrent,OneHourEnergy,Energy,i,k,f):
    global data15_1
    global Volt15_1
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
    #global OneHourEnergy_1
    global optionsave
    #global vt115
    global MaxAparentPower_1
    basea = datetime.datetime.now()
    if(basea.minute==0 or basea.minute==1 or basea.minute==2 or basea.minute==15 or basea.minute==16 or basea.minute==17 or basea.minute==30 or basea.minute==31 or basea.minute==32 or basea.minute==45 or basea.minute==46 or basea.minute==47): 
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
                    data15_1.insert(34,OneHourEnergy)
                    data15_1.insert(35,Energy)
                    data15_1.insert(0,datetime.datetime.now())
                    workbook=openpyxl.load_workbook(filename = dest_filename)
                    sheet2 = workbook[f"15Min-{k}-{f}"]
                    dict = { "time":timest(),"MaxVoltage15_1": MaxVoltage15_1, "MeanVoltage15_1": MeanVoltage15_1,"MinVoltage15_1":MinVoltage15_1,
                    "MaxCurrent15_1": MaxCurrent15_1, "MeanCurrent15_1": MeanCurrent15_1,"MinCurrent15_1":MinCurrent15_1,
                    "MaxActivePower_1":MaxActivePower_1,"MeanActivePower_1":MeanActivePower_1,"MinActivePower_1":MinActivePower_1,
                    "MaxReactivePower_1":MaxReactivePower_1,"MeanReactivePower_1":MeanReactivePower_1,"MinReactivePower_1":MinReactivePower_1,
                    "MaxAparentPower_1":MaxAparentPower_1,"MeanAparentPower_1":MeanAparentPower_1,"MinAparentPower_1":MinAparentPower_1,
                    "MaxFPInductive_1":MaxFPInductive_1,"MeanFPInductive_1":MeanFPInductive_1,"MinFPInductive_1":MinFPInductive_1,
                    "MaxFPReactive_1":MaxFPReactive_1,"MeanFPReactive_1":MeanFPReactive_1,"MinFPReactive_1":MinFPReactive_1,
                    "MaxFDVoltage_1":MaxFDVoltage_1,"MeanFDVoltage_1":MeanFDVoltage_1,"MinFDVoltage_1":MinFDVoltage_1,
                    "MaxFDCurrent_1":MaxFDCurrent_1,"MeanFDCurrent_1":MeanFDCurrent_1,"MinFDCurrent_1":MinFDCurrent_1,
                    "MaxDATVoltage_1":MaxDATVoltage_1,"MeanDATVoltage_1":MeanDATVoltage_1,"MinDATVoltage_1":MinDATVoltage_1,
                    "MaxDATCurrent_1":MaxDATCurrent_1,"MeanDATCurrent_1":MeanDATCurrent_1,"MinDATCurrent_1":MinDATCurrent_1,
                    "OneHourEnergy_1":OneHourEnergy,"Energy_1":Energy
                    }
                    sheet2.append(list(data15_1))
                    print(f'Data 1: Guardando Promedios')
                    #optionsave=1
                    ##SendDataToBroker(q=i,k=k,f=f,VoltajeMax=f'{MaxVoltage15_1}',VoltajePromedio=f'{MeanVoltage15_1}',VoltajeMin=f'{MinVoltage15_1}',MaxCorriente=f'{MaxCurrent15_1}',PromedioCorriente=f'{MeanCurrent15_1}',MinimoCorriente=f'{MinCurrent15_1}',PotenciaMax=f'{MaxAparentPower_1}',PromedioPotenciaAparente=f'{MeanAparentPower_1}',MinPotenciaAparente=f'{MinAparentPower_1}',Energia=f'{Energy}')
                    ###SendDataToBroker(MaxVoltage15_1,MeanVoltage15_1,MinVoltage15_1,MaxCurrent15_1,MeanCurrent15_1,MinCurrent15_1,MaxAparentPower_1,MeanAparentPower_1,MinAparentPower_1,OneHourEnergy,Energy,k,f,_)
                    list_data_db_insert(dict)
                    #vt115=time.time()
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
                    #OneHourEnergy_1=0
               elif(Access_1==1):
                    #print("paso elif 2")
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
        
        if(len(Volt15_1)>4):
            Volt15_1.sort()
            indice=np.argmin(Volt15_1)
            Volt15_1.pop(indice+1)
            indice=np.argmax(Volt15_1)
            Volt15_1.pop(indice-1)
        if(len(Current15_1)>4):
            Current15_1.sort()
            indice=np.argmin(Current15_1)
            Current15_1.pop(indice+1)
            indice=np.argmax(Current15_1)
            Current15_1.pop(indice-1)
        if(len(ActivePower15_1)>4):
            ActivePower15_1.sort()
            indice=np.argmin(ActivePower15_1)
            ActivePower15_1.pop(indice+1)
            indice=np.argmax(ActivePower15_1)
            ActivePower15_1.pop(indice-1)
        if(len(ReactivePower15_1)>4):
            ReactivePower15_1.sort()
            indice=np.argmin(ReactivePower15_1)
            ReactivePower15_1.pop(indice+1)
            indice=np.argmax(ReactivePower15_1)
            ReactivePower15_1.pop(indice-1)
        if(len(AparentPower15_1)>4):
            AparentPower15_1.sort()
            indice=np.argmin(AparentPower15_1)
            AparentPower15_1.pop(indice+1)
            indice=np.argmax(AparentPower15_1)
            AparentPower15_1.pop(indice-1)
        if(len(FP15_Reactive_1)>4):
            FP15_Reactive_1.sort()
            indice=np.argmin(FP15_Reactive_1)
            FP15_Reactive_1.pop(indice+1)
            indice=np.argmax(FP15_Reactive_1)
            FP15_Reactive_1.pop(indice-1)
        if(len(FP15_Inductive_1)>4):
            FP15_Inductive_1.sort()
            indice=np.argmin(FP15_Inductive_1)
            FP15_Inductive_1.pop(indice+1)
            indice=np.argmax(FP15_Inductive_1)
            FP15_Inductive_1.pop(indice-1)
        if(len(FDVoltage15_1)>4):
            FDVoltage15_1.sort()
            indice=np.argmin(FDVoltage15_1)
            FDVoltage15_1.pop(indice+1)
            indice=np.argmax(FDVoltage15_1)
            FDVoltage15_1.pop(indice-1)
        if(len(FDCurrent15_1)>4):
            FDCurrent15_1.sort()
            indice=np.argmin(FDCurrent15_1)
            FDCurrent15_1.pop(indice+1)
            indice=np.argmax(FDCurrent15_1)
            FDCurrent15_1.pop(indice-1)
        if(len(DAT15Voltage_1)>4):
            DAT15Voltage_1.sort()
            indice=np.argmin(DAT15Voltage_1)
            DAT15Voltage_1.pop(indice+1)
            indice=np.argmax(DAT15Voltage_1)
            DAT15Voltage_1.pop(indice-1)
        if(len(DAT15Current_1)>4):
            DAT15Current_1.sort()
            indice=np.argmin(DAT15Current_1)
            DAT15Current_1.pop(indice+1)
            indice=np.argmax(DAT15Current_1)
            DAT15Current_1.pop(indice-1)