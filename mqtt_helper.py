import random
import requests
import json
import time
import paho.mqtt.client as mqtt
import datetime


broker = '192.168.43.74'
port = 1883
dId = '63c9604b1'
passw = 'g6U8VzfBPg'
webhook_endpoint = 'http://192.168.43.74:3001/api/getdevicecredentials'
userId = "647c0c2e463c9604b1b96e5d"
topicmqtt = userId + dId

rcConnect = 1
client = None
data = None 
def get_mqtt_credentials():
    global data
    print("Getting MQTT Credentials from WebHook")
    time.sleep(2)
    toSend = {"dId": dId, "password": passw}
    respuesta = requests.post(webhook_endpoint, data=toSend)

    if respuesta.status_code != 200:
        print("Error in response ", respuesta.status_code)
        respuesta.close()
        return None

    print("Mqtt Credentials Obtained Successfully :)   ")
    my_bytes_value = respuesta.content
    my_new_string = my_bytes_value.decode("utf-8").replace("'", '"')
    data = json.loads(my_new_string)
    s = json.dumps(data, indent=4, sort_keys=True)
    print(s)
    respuesta.close()
    print("Ends mqtt credentials")
    return {
        "username": data["username"],
        "password": data["password"],
        "topic": data["topic"] + "+/actdata",
        "client_id": f'device_{dId}_{random.randint(0, 9999)}',
    }, data


def on_disconnect(client, userdata, rc):
    if rc != 0 and rc != 5:
        print("Unexpected disconnection, will auto-reconnect")
    elif rc == 5:
        print("Getting new credentials!")
        mqtt_credentials, _ = get_mqtt_credentials()
        if mqtt_credentials is not None:
            client.username_pw_set(mqtt_credentials["username"], mqtt_credentials["password"])


def on_connected(client, userdata, flags, rc, mqtt_credentials):
    if rc == 0:
        client.connected_flag = True
        client.subscribe(mqtt_credentials["topic"])
        print("connected OK")
        print("rc =", client.connected_flag)
    else:
        print("Bad connection Returned code=", rc)
        client.bad_connection_flag = False


def ConnectToBroker():
    global client
    mqtt_credentials, _ = get_mqtt_credentials()
    if mqtt_credentials is None:
        print("Failed to get MQTT credentials!")
        return
    client = mqtt.Client(mqtt_credentials["client_id"])
    client.connect(broker, port)
    client.on_disconnect = on_disconnect
    client.username_pw_set(mqtt_credentials["username"], mqtt_credentials["password"])
    client.on_connect = lambda client, userdata, flags, rc: on_connected(client, userdata, flags, rc, mqtt_credentials)
    client.loop_start()



optionsave=0	
k1="REDCompañia"	
k2="CentralFotovoltaica"	
k3="ConsumoCliente"	
f1="Fase-1"	
f2="Fase-2"	
f3="Fase-3"
vttime=time.time()

def SendDataToBroker( vt, data, variable, categoryName, **kwargs):
    global vttime
    vttime = vt
    print("Entrando SendDataToBroker ")
    #print(data)
    def publish(client):
        global vttime
        timeToSend = time.time()

        for key, value in kwargs.items():
            str_num = {"value": value, "save": optionsave}

            if isinstance(value, datetime.datetime):
                str_num["value"] = value.isoformat()

            valueJson = json.dumps(str_num)
            print(f' A enviar: {key} {value} {valueJson}')
            for i in data["variables"]:
                variable_full_name_key = False
                variable_key = None

                if i["variableFullName"] == key:
                    print("es igual 1")
                    variable_full_name_key = True
                elif "variableFullName2" in i and i["variableFullName2"] == key:
                    print("es igual 2")
                    variable_full_name_key = True
                elif "variableFullName3" in i and i["variableFullName3"] == key:
                    print("es igual 3")
                    variable_full_name_key = True

                if variable_full_name_key:
                    print(f"Preparando Envio en publish de variable {key} {value} {timeToSend - vttime}")
                    freq = i["variableSendFreq"]

                    #if timeToSend - vttime > float(freq):
                    topic = data["topic"] + variable + "/" + key + "/sdata"
                    result = client.publish(topic, valueJson)
                    status = result[0]
                    if status == 0:
                        print(f"Send {key} - {value} - {topic}")
                    else:
                        print(f"Failed to send message to topic {topic}")
                    vttime = time.time()

    publish(client)

    return vttime

