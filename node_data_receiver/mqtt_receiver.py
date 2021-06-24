import paho.mqtt.client as mqtt
from sys import getsizeof
from datetime import datetime
from json import loads, dumps
from confluent_kafka import Producer
import socket
import logging

logging.basicConfig(filename='mqtt_receiver.log',filemode="w", level=logging.INFO)
with open("config.json","r") as f:
	config = loads(f.read())

producer = Producer(config['producer_config'])
producer.flush()


def on_connect(client, userdata, flags, rc):
    #logging.info(" Connected at: "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S")+" with result code "+str(rc))
    client.subscribe(config['mqtt_topics']['data_node'])    


def on_message(client, userdata, msg):
		node_values = eval(msg.payload.decode('utf-8'))
		logging.info("Received at: "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S")+" Topic: "+str(msg.topic)+" Device ID: "+node_values["Device ID"])
		
		node_values['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")		
		node_values["Logs"]['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")		

		#print(node_values)
		try:		
			producer.produce(config['kafka_topics']['logs_node'],key=node_values['Device ID'], value=dumps(node_values['Logs']))				
			logging.info("Sent to "+config['kafka_topics']['logs_node']+" at "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
			del node_values["Logs"]

			#print(node_values)
			producer.produce(config['kafka_topics']['data_node'],key=node_values['Device ID'], value=dumps(node_values))		
			logging.info("Sent to "+config['kafka_topics']['data_node']+" at "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
		except Exception as e:
			logging.error(f"Unable to send due to {str(e)} at "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))	

logging.info("Started listening")
client = mqtt.Client("frizzle_receiver")
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("frizzle_test", "FRIZZLE")
client.connect(config['mqtt_server'], 1883, 60)
client.loop_forever()
