import paho.mqtt.client as mqtt
from sys import getsizeof
from datetime import datetime
from json import loads, dumps
from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "13.126.242.56:9092",'client.id':'13.126.242.56'}
producer = Producer(conf)
producer.flush()
mqttServer = "13.126.242.56"
topics = ['node_data']
def on_connect(client, userdata, flags, rc):
    print("Connected with result code" + str(rc))
    client.subscribe("Frizzle/Sensor_Data")


def on_message(client, userdata, msg):
	node_sensor_values = eval(msg.payload.decode('utf-8'))
	if 'time-stamp' not in node_sensor_values.keys():
		node_sensor_values['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
	producer.produce(topics[0],key=node_sensor_values['Device ID'], value=dumps(node_sensor_values))
	print("sent to ",topics[0])


client = mqtt.Client("rishi_bhowmi_receiver")
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("frizzle_test", "FRIZZLE")
client.connect(mqttServer, 1883, 60)
client.loop_forever()
