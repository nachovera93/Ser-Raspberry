import requests
import datetime
import json
import random
import time
import paho.mqtt.client as mqtt
import os
import time
"""
    0: connection succeeded
    1: connection failed - incorrect protocol version
    2: connection failed - invalid client identifier
    3: connection failed - the broker is not available
    4: connection failed - wrong username or password
    5: connection failed - unauthorized
    6-255: undefined
    """

broker = '192.168.100.121'    #mqtt server
port = 1883
dId = '123456789'
passw = 'oa5iFKhDjc'
#dId2 = '12344321'
#passw2 = 'yFJMESnzxl'
webhook_endpoint = 'http://192.168.100.121:3001/api/getdevicecredentials'


 
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


def get_mqtt_credentials2():
    global usernamemqtt2
    global passwordmqtt2
    global mqttopic2
    global str_client_id2
    global topicmqtt2
    global data2
    print("Getting MQTT Credentials from WebHook2")
    time.sleep(2)
    toSend2 = {"dId": dId2, "password": passw2}
    respuesta2 = requests.post(webhook_endpoint, data=toSend2)

    if(respuesta2.status_code < 0):
          print("Error Sending Post Request ", respuesta2.status_code)
          respuesta2.close()
          return False
    if(respuesta2.status_code != 200):
          print("Error in response ", respuesta2.status_code)
          respuesta2.close()
          return False
    if(respuesta2.status_code == 200):
          print("Mqtt Credentials 2 Obtained Successfully :)   ")
          #print("json: " ,resp.content)
          #print('- ' * 20)
          my_bytes_value2 = respuesta2.content      #Contenido entero del json
          my_new_string2 = my_bytes_value2.decode("utf-8").replace("'", '"')
          data2 = json.loads(my_new_string2)
          s2 = json.dumps(data2, indent=4, sort_keys=True)
          #print(s)
          usernamemqtt2 = data2["username"]
          #print("username:",usernamemqtt)
          passwordmqtt2 = data2["password"]
          topicmqtt2 = data2["topic"]   #topico al que nos vamos a suscribir
          mqttopic2 = f"{topicmqtt}+/actdata"
          str_client_id2 = f'device_{dId}_{random.randint(0, 9999)}'
          #print(mqttopic)
          respuesta2.close()
          print("Ends mqtt credentials 2")
    return True


get_mqtt_credentials()
#get_mqtt_credentials2()
   
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

def on_disconnect2(client, userdata, rc):
    if (rc != 0 and rc != 5):
        print("Unexpected disconnection, will auto-reconnect")
    elif(rc==5):
        print("Getting new credentials!")
        get_mqtt_credentials2()
        client2.username_pw_set(usernamemqtt2, passwordmqtt2)
                      
# The callback for when the client receives a CONNACK response from the server.
def on_connected2(client, userdata, flags, rc):
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    if rc==0:
        client2.connected_flag=True #set flag
        client2.subscribe(mqttopic2)
        print("connected OK")
        print("rc 2 =",client2.connected_flag)
    else:
        print("Bad connection Returned code=",rc)
        client2.bad_connection_flag=False
   
       
client = mqtt.Client(str_client_id)   #Creación cliente
client.connect(broker, port)     #Conexión al broker
client.on_disconnect = on_disconnect
client.username_pw_set(usernamemqtt, passwordmqtt)
client.on_connect = on_connected
client.loop_start()
time.sleep(5)
#client2 = mqtt.Client(str_client_id2)   #Creación cliente
#client2.connect(broker, port)     #Conexión al broker
#client2.on_disconnect = on_disconnect2
#client2.username_pw_set(usernamemqtt2, passwordmqtt2)
#client2.on_connect = on_connected2
#client2.loop_start()



def Humedad():
    global humedad
    num = random.randint(0, 50)
    str_num = {"value":num,"save":1}
    humedad = json.dumps(str_num)
    #return msg 

def Temperature():
    global temperature
    num = random.randint(0, 50)
    str_num = {"value":num,"save":1}
    temperature = json.dumps(str_num)
    #return msg    

b=time.time()
c=time.time()
z=time.time()
x=time.time()

def publish(client):
    
        global b, c 
        a=time.time()
        for i in data["variables"]:

            #    if(data["variables"][i]["variableType"]=="output"):
            #        continue
            if(i["variableFullName"]=="Voltaje"):
                freq = i["variableSendFreq"]
                if(a - b > float(freq)):
                     b=time.time()
                     str_variable = i["variable"]
                     topic1 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic1, humedad)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send humedad: `{humedad}` to topic `{topic1}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic1}")
        
                   
            if(i["variableFullName"]=="Voltajeas"):
                freq = i["variableSendFreq"]
                if(a - c > freq):
                     #print("varlastsend 1: ",varsLastSend[i])
                     c=time.time()
                     str_variable2 = i["variable"]
                     topic2 = topicmqtt + str_variable2 + "/sdata"
                     result = client.publish(topic2, temperature)
                     status = result[0]
                     if status == 0:
                         print(f"Send temperatura: `{temperature}` to topic `{topic2}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic2}")
            
        
        """
        str_variable = data["variables"][0]["variable"]
        print("data:",str_variable)
        topic1 = topicmqtt + str_variable + "/sdata"
        #msg=randomnum2()
        result = client.publish(topic1, humedad)
        #result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{humedad}` to topic `{topic1}`")
        else:
            print(f"Failed to send message to topic {topic1}")
        """ 


def publish2(client2):
    
        global z, x 
        h=time.time()
        for i in data2["variables"]:

            #    if(data["variables"][i]["variableType"]=="output"):
            #        continue
            if(i["variableFullName"]=="Corriente-CGE"):
                freq = i["variableSendFreq"]
                if(h - z > freq):
                     z=time.time()
                     str_variable = i["variable"]
                     topic = topicmqtt2 + str_variable + "/sdata"
                     result = client2.publish(topic, humedad)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send humedad: `{humedad}` to topic `{topic}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic}")
       
                   
    
    
  
while client.connected_flag:
        Humedad()
        Temperature()
        publish(client)
        #publish2(client2)
        #publish(client2)
        time.sleep(5)
    

print("client stop")  # no esta pasando en ninguna situación
client.loop_stop()  



"""
{
    "password": "7dnkOzqUwL",
    "topic": "610a8d733d1a8852b4e6b775/12345678/",
    "username": "CRE6Gpub8q",
    "variables": [
        {
            "variable": "O1DM08hKvJ",
            "variableFullName": "Voltaje",
            "variableSendFreq": 10,
            "variableType": "input"
        },
        {
            "variable": "mmdKbcHfSA",
            "variableFullName": "Corriente",
            "variableSendFreq": 15,
            "variableType": "input"
        }
    ]
}
"""
