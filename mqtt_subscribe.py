import paho.mqtt.client as mqtt
from sys import getsizeof
from datetime import datetime
from json import loads, dumps
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=[
                         '13.232.244.184:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))


def on_connect(client, userdata, flags, rc):
    print("Connected with result code" + str(rc))
    client.subscribe("testing_topic")


def on_message(client, userdata, msg):
	# node_sensor_values = loads(msg.payload)
	# node_sensor_values = loads(msgpayload)
	#node_sensor_values['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
	#print(node_sensor_values)
	node_sensor_values = msg.payload.decode('utf-8')
	print(node_sensor_values)
	producer.send('node-1', value=node_sensor_values)
    #print(msg.topic+" "+str(msg.payload))


mqttServer = "65.1.190.134"

client = mqtt.Client("rishi_bhowmi_receiver")
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("frizzle_test", "FRIZZLE")
client.connect(mqttServer, 1883, 60)
client.loop_forever()	
