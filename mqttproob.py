
import random
import time
import json
from paho.mqtt import client as mqtt
import requests
"""
broker = '192.168.1.85'
port = 1883
topic = "Nacho/9999/var22/sdata"
topic2 = "Nacho/8888/var11/actdata"
topic3 = "Nacho/7777/var33/sdata"
topic4 = "Nacho/5555/var55/sdata"
topic5 = "Nacho/5555/var554/sdata"
#topic2 = "#"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'superuser'
password = 'superuser'
"""
broker = 'localhost'   #'192.168.1.85' #mqtt server
port = 1883
dId = '12345321'
passw = 'pbkkuGaxzE'
webhook_endpoint = 'http://localhost:3001/api/getdevicecredentials'


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
          passwordmqtt = data["password"]
          topicmqtt = data["topic"]   #topico al que nos vamos a suscribir
          mqttopic = f"{topicmqtt}+/actdata"
          str_client_id = f'device_{dId}_{random.randint(0, 9999)}'
          print(mqttopic)
          respuesta.close()
          print("Ends mqtt credentials")
    return True


rcConnect = 1  
def on_disconnect(client, userdata, rc):
    if (rc != 0 and rc != 5):
        print("Unexpected disconnection, will auto-reconnect")
    elif(rc==5):
        print("Getting new credentials!")
        #get_mqtt_credentials()
        client.username_pw_set(usernamemqtt, passwordmqtt)
 
                     
def on_connected(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        client.subscribe(mqttopic)
        print("connected OK")
        print("rc =",client.connected_flag)
    else:
        global rcConnect
        rcConnect=rcConnect+1
        print(f"rcConnection = {rcConnect}")
        print("Bad connection Returned code=",rc)
        client.bad_connection_flag=False

get_mqtt_credentials()     
client = mqtt.Client(str_client_id)   #Creación cliente
client.connect(broker, port)     #Conexión al broker
client.on_disconnect = on_disconnect
client.username_pw_set(usernamemqtt, passwordmqtt)
client.on_connect = on_connected
client.loop_start()

"""
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_count = 0
    while True:
        time.sleep(15)
        #msg = f"value: {msg_count}"
        str_num = {"value":msg_count}
        str_num_on = {"value": 1}
        str_num_off = {"value": 0}
        valueJson = json.dumps(str_num)
        valueJson2 = json.dumps(str_num_on)
        valueJson3 = json.dumps(str_num_off)
        valueJson4 = json.dumps(str_num)
        valueJson5 = json.dumps(str_num)
        result = client.publish(topic, valueJson)
        # result: [0, 1]
        status = result[0]           
        if status == 0:
            print(f"Send `{msg_count}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        
        # result: [0, 1]
        if msg_count % 2 == 0:
            result = client.publish(topic3, valueJson2)
            status = result[0]           
            if status == 0:
                print(f"Send ON to topic `{topic3}`")
            else:
                print(f"Failed to send message to topic {topic3}")
            result = client.publish(topic4, valueJson4)
            status = result[0]           
            if status == 0:
                print(f"Send `{msg_count}` to topic `{topic4}`")
            else:
                print(f"Failed to send message to topic {topic4}")
        else:
            result = client.publish(topic3, valueJson3)
            status = result[0]           
            if status == 0:
                print(f"Send OFF to topic `{topic3}`")
            else:
                print(f"Failed to send message to topic {topic3}")
            result = client.publish(topic5, valueJson5)
            status = result[0]           
            if status == 0:
                print(f"Send `{msg_count}` to topic `{topic5}`")
            else:
                print(f"Failed to send message to topic {topic5}")
       
            
        msg_count += 1

def subscribe(client: mqtt):
    def on_message(client, userdata, msg):
        if (msg.payload.decode() == '{"value":true}'):
            print("Luz Encendida")
        elif (msg.payload.decode() == '{"value":false}'):
            print("Luz Apagada")
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        
    client.subscribe(topic2)
    client.on_message = on_message
"""   
def run():
    #client = connect_mqtt()
    #subscribe(client)
    client.loop_start()
    #publish(client)


if __name__ == '__main__':
    run()