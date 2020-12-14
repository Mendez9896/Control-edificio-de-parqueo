
# merge and rearrangement of original emqx code by TxOs.

import random
import time

from paho.mqtt import client as mqtt_client 

#configuration
broker = 'broker.hivemq.com'
port = 1883
publishTopic="rams/publish2"
subscribeTopic="rams/subscribe"

msg_count = 1

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'

# estructura = [{"1A": 1, "2A": 1},{},{},{},{}]
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


def run():
    # print(estructura2["12345"][0])
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()
