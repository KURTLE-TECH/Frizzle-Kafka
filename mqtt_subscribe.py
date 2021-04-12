import paho.mqtt.client as mqtt
from sys import getsizeof
from datetime import datetime
from json import loads, dumps
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=[
                          '13.126.242.56:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))


def on_connect(client, userdata, flags, rc):
    print("Connected with result code" + str(rc))
    client.subscribe("Frizzle/Sensor_Data")


def on_message(client, userdata, msg):
	node_sensor_values = eval(msg.payload.decode('utf-8'))
	# print(type(node_sensor_values))
	if 'time-stamp' not in node_sensor_values.keys():
		node_sensor_values['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
	print(node_sensor_values)
	producer.send('node-1', value=node_sensor_values)


mqttServer = "13.126.242.56"

client = mqtt.Client("rishi_bhowmi_receiver")
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("frizzle_test", "FRIZZLE")
client.connect(mqttServer, 1883, 60)
client.loop_forever()	
