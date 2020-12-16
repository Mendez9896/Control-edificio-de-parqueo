
# merge and rearrangement of original emqx code by TxOs.

import random
import time

from paho.mqtt import client as mqtt_client 

#configuration
broker = 'broker.hivemq.com'
port = 1883

subscribeCubil = "2587/parqueo/cubil"
subscribeCubilEstadoInfo = "2587/parqueo/cubil/estado/info"
publishCubilEstado= "2587/parqueo/cubil/estado"

publishTopic="rams/publish2"
publishPisosDisponibles="2587/server/disponibles_pisos"
publishTopicEstadoCubil="2587/parqueo/cubil/estado"

publishPisoTopic="2587/server/disponibles_piso"
subscribeTopic="rams/subscribe"
subscribeTopicApp="2587/App"

subscribeTopicEstadoCubil="2587/parqueo/cubil/estado/take"

msg_count = 1

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

estructura = [{},{},{},{},{}]
# piso = 1
# estructura[piso-1]["1A"]

estructura2 = {}

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



def cubilSubscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic == "2587/parqueo/cubil":
            (piso,cubil,ocupado) = str(msg.payload.decode()).split(",")
            estructura[int(piso)-1][str(cubil)]=int(ocupado)
            # print(estructura[int(piso)-1][str(cubil)])
        elif msg.topic == "2587/parqueo/cubil/estado/info":
            allParkingSpaces = str(msg.payload.decode()).split("|")
            for i in allParkingSpaces:
                if str(i) != "":
                    (piso,cubil,ocupado) = str(i).split(",")
                    estructura[int(piso)-1][str(cubil)]=int(ocupado)
            # print(estructura)
    client.subscribe(subscribeCubil)
    client.subscribe(subscribeCubilEstadoInfo)
    client.on_message = on_message


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
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
                if comand[1].isnumeric():
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
            elif comand[0] == "ocupar" :
                nd = {comand[3]: [comand[1],comand[2]]}
                estructura2.update(nd)
                print(estructura2)
            elif comand[0] == "buscar" :
                keys = estructura2.keys()
                if comand[1] in keys :
                    publishBuscarParqueo(client, estructura2.get(comand[1]))
                else:
                    publishBuscarParqueo(client, 0)
        elif msg.topic == "2587/parqueo/cubil":
            (piso,cubil,ocupado) = str(msg.payload.decode()).split(",")
            estructura[int(piso)-1][str(cubil)]=int(ocupado)
            # print(estructura[int(piso)-1][str(cubil)])
        elif msg.topic == "2587/parqueo/cubil/estado/info":
            allParkingSpaces = str(msg.payload.decode()).split("|")
            if len(allParkingSpaces) > 1:
                for i in allParkingSpaces:
                    if str(i) != "":
                        (piso,cubil,ocupado) = str(i).split(",")
                        estructura[int(piso)-1][str(cubil)]=int(ocupado)
            # print(estructura)
    client.subscribe(subscribeTopicApp)
    client.subscribe(subscribeTopicEstadoCubil)
    client.subscribe(subscribeCubil)
    client.subscribe(subscribeCubilEstadoInfo)
    client.on_message = on_message



def publishBuscarParqueo(client, lugar):
    time.sleep(1)
    publishTopic = "2587/parqueo/buscar"
    if lugar == 0 :
        msg = 0
    else:
        msg = lugar[0] + "," + lugar[1]
    
    result = client.publish(publishTopic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{publishTopic}`")
    else:
        print(f"Failed to send message to topic {publishTopic}")


def publishPisos(client,floors):    
    global msg_count
    time.sleep(1)
    msg = f""
    counter=1
    for key in floors:
        msg += str(counter)+","+str(key)
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

def cubilEstadoPublish(client):
    result = client.publish(publishCubilEstado,"0")
    status = result[0]
    if status == 0:
        print(f"Send 0 to topic `{publishCubilEstado}`")
    else:
        print(f"Failed to send message to topic {publishCubilEstado}")  

def run():
    
    client = connect_mqtt()
    # subscribe(client)
    cubilEstadoPublish(client)
    subscribeApp(client)
    # cubilEstadoPublish(client)
    #cubilSubscribe(client)
    #subscribeCubilEstado(client)
    client.loop_forever()



if __name__ == '__main__':
    run()
