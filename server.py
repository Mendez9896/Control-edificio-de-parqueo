
# merge and rearrangement of original emqx code by TxOs.

import random
import time

from paho.mqtt import client as mqtt_client 

#configuration
broker = 'broker.hivemq.com'
port = 1883

publishTopic="rams/publish2"

publishPisosDisponibles="2587/server/disponibles_pisos"
publishTopicEstadoCubil="2587/parqueo/cubil/estado/info"

publishPisoTopic="2587/server/disponibles_piso"
subscribeTopic="rams/subscribe"
subscribeTopicApp="2587/App"

subscribeTopicEstadoCubil="2587/parqueo/cubil/estado"

msg_count = 1

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

estructura = [{"1A": 0, "2A": 1},{"1A": 0, "2A": 1},{"1A": 0, "2A": 0},{"1A": 0, "2A": 1},{"1A": 0, "2A": 1}]
# piso = 1
# estructura[piso-1]["1A"]

# estructura2 = {"12345":[1,2],"654321": [2,4]}

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            print("publishTopic:'"+publishTopic+"'")
            print("subscribeTopic:'"+subscribeTopic+"'")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        publish(client)
    client.subscribe(subscribeTopic)
    client.on_message = on_message


def subscribeApp(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic==subscribeTopicApp:
            print(f"Received subscribetopic")
            comand = msg.payload.decode().split(",")
            floors=[0]*5
            counter=0
            if comand[0]=="disponibles_pisos":
                for floor in estructura:
                    print(f"{floor}")
                    for key in floor:
                        print(f"{key} {floor[key]}")
                        if floor[key] == 0:
                            floors[counter]+=1
                    counter+=1
                publishPisos(client,floors)
            elif comand[0]=="disponible_piso":
                piso= int(comand[1])-1
                msg=str(comand[1])+"|"
                msgcounter=0
                msgcubil=""
                for key in estructura[piso]:
                    if estructura[piso][key] == 0:
                        msgcubil+=key+"|"
                        msgcounter+=1
                msg+=str(msgcounter)+"|"
                msg+=msgcubil
                msg=msg[:-1]
                publishPiso(client,msg)
        elif msg.topic==subscribeTopicEstadoCubil:
            estado = msg.payload.decode().split(",")
            piso= int(estado[0])-1
            cubil= estado[1]
            msg=estado[0]+","+estado[1]+","
            if estructura[piso][cubil]==0:
                estructura[piso][cubil] = 1
                msg+=str(1)+","+"ok"
            else:
                msg+=str(1)+","+"Nok"
            publishEstadoCubil(client,msg)
    client.subscribe(subscribeTopicApp)
    client.subscribe(subscribeTopicEstadoCubil)
    client.on_message = on_message


def publish(client):    
    global msg_count
    time.sleep(1)
    msg = f"OK"
    result = client.publish(publishTopic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{publishTopic}`")
    else:
        print(f"Failed to send message to topic {publishTopic}")
    msg_count += 1

def publishPisos(client,floors):    
    global msg_count
    time.sleep(1)
    msg = f""
    counter=1
    for key in floors:
        msg += str(counter)+","+str(floors[key])
        if counter!=5:
            msg+="|"
        counter+=1

    result = client.publish(publishPisosDisponibles, msg)
    
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{publishPisosDisponibles}`")
    else:
        print(f"Failed to send message to topic {publishPisosDisponibles}")
    msg_count += 1

def publishPiso(client,msg2):    
    global msg_count
    time.sleep(1)
    result = client.publish(publishPisoTopic, msg2)
    status = result[0]
    if status == 0:
        print(f"Send `{msg2}` to topic `{publishPisoTopic}`")
    else:
        print(f"Failed to send message to topic {publishPisoTopic}")
    msg_count += 1

def publishEstadoCubil(client,msg2):    
    global msg_count
    time.sleep(1)
    result = client.publish(publishTopicEstadoCubil, msg2)
    status = result[0]
    if status == 0:
        print(f"Send `{msg2}` to topic `{publishTopicEstadoCubil}`")
    else:
        print(f"Failed to send message to topic {publishTopicEstadoCubil}")
    msg_count += 1

def run():
    # print(estructura2["12345"][0])
    client = connect_mqtt()
    #subscribe(client)
    subscribeApp(client)
    #subscribeCubilEstado(client)
    #client.loop_forever()
    while True:
        client.loop()
        time.sleep(5)
        


if __name__ == '__main__':
    run()
