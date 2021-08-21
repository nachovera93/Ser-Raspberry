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

broker = '192.168.100.177'    #mqtt server
port = 1883
dId = '12345678'
passw = 'a0r5Y6Gs0O'
webhook_endpoint = 'http://192.168.100.177:3001/api/getdevicecredentials'


 
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
varsLastSend=[b,c]

def publish(client):
    
        global b, c ,varsLastSend
        a=time.time()
        for i in data["variables"]:

            #    if(data["variables"][i]["variableType"]=="output"):
            #        continue
            if(i["variableFullName"]=="Corriente"):
                freq = i["variableSendFreq"]
                if(a - b > freq):
                     b=time.time()
                     str_variable = i["variable"]
                     topic1 = topicmqtt + str_variable + "/sdata"
                     result = client.publish(topic1, humedad)
                     status = result[0]
                     
                     if status == 0:
                         print(f"Send humedad: `{humedad}` to topic `{topic1}` con freq: {freq}")
                     else:
                         print(f"Failed to send message to topic {topic1}")
        
                   
            if(i["variableFullName"]=="Voltaje"):
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


        
    
  
while client.connected_flag: 
    #print("In loop")
    Humedad()
    Temperature()
    publish(client)
    #time.sleep(5)
    

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
