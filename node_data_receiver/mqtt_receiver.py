import paho.mqtt.client as mqtt
from sys import getsizeof
from datetime import datetime
from json import loads, dumps
from database import DynamodbHandler as db
import time
import socket
import logging
import boto3
import redis

with open("config.json", "r") as f:
    config = loads(f.read())

write_client = boto3.client('timestream-write', region_name='us-east-1')

redis_cluster_endpoint = redis.Redis(
    host=config["redis_host"],
    port=config["redis_port"],
    db=0)

logging.basicConfig(filename='mqtt_receiver.log',filemode="w", level=logging.INFO)
with open("config.json","r") as f:
	config = loads(f.read())

db_handler = db.DynamodbHandler()


def on_connect(client, userdata, flags, rc):
    #logging.info(" Connected at: "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S")+" with result code "+str(rc))
    client.subscribe(config['mqtt_topics']['data_node'])    


def on_message(client, userdata, msg):
		node_values = eval(msg.payload.decode('utf-8'))
		logging.info("Received at: "+datetime.now().strftime("%Y-%m-%d_%H:%M:%S")+" Topic: "+str(msg.topic)+" Device ID: "+node_values["Device ID"])
		curr_time = datetime.now()
		node_values['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")		
		node_values["Logs"]['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")		

		print(node_values)
		logs_data = node_values["Logs"]
		del node_values["Logs"]		
		# topic = msg.topic()
		# key = msg.key().decode("utf-8")

		node_id = node_values['Device ID']
		try:
		
			response = db_handler.insert(node_values, node_id)
			logging.info("Dynamo insertion of "+node_id +
							" with response "+response+" at: "+curr_time.strftime("%Y-%m-%d_%H:%M:%S"))
		except Exception as e:
			#         	print(e)
			logging.error(" Dynamo error insertion error " +
							str(e)+" at: "+curr_time.strftime("%Y-%m-%d_%H:%M:%S"))
		
		#inserting logs
		try:		
			response = db_handler.insert(logs_data, node_id+"_logs")
			logging.info("Dynamo insertion of "+node_id+"_logs" +
							" with response "+response+" at: "+curr_time.strftime("%Y-%m-%d_%H:%M:%S"))
		except Exception as e:			
			logging.error(" Dynamo error insertion error " +
							str(e)+" at: "+curr_time.strftime("%Y-%m-%d_%H:%M:%S"))


		# updating the real time database
		try:
			redis_cluster_endpoint.set(node_id, dumps(node_values))
			# redis_cluster_endpoint.lpush(table_name, dumps(msg_dict))
			logging.info("Redis insertion of " +
							node_id+" at: "+curr_time.strftime("%Y-%m-%d_%H:%M:%S"))
		except Exception as e:
			logging.info("Tried at "+curr_time +
							" Redis insertion error "+str(e))
		
		# updating 24 data database
		try:
			dimensions = [
				{'Name': 'Device ID', 'Value': node_values["Device ID"]}, ]

			current_time = str(int(round(time.time() * 1000)))     
			node_values.pop("picture")
			node_data_timestream = {
				'Dimensions': dimensions,
				'MeasureName': 'Node Data',
				'MeasureValue': dumps(node_values),
				'MeasureValueType': 'VARCHAR',
				'Time': current_time
			}

			records = [node_data_timestream]
			try:
				result = write_client.write_records(
					DatabaseName='Frizzle_Realtime_Database', TableName=node_values["Device ID"], Records=records, CommonAttributes={})
				# print("WriteRecords Status: [%s]" %
				#     result['ResponseMetadata']['HTTPStatusCode'])
				logging.info(f"Successully put {node_values['Device ID']} into Timestream")
			except Exception as e:
				logging.error(f"Insertion error for {node_values['Device ID']} into Timestream with {str(e)}")
		
		except Exception as e:
			logging.error(f"Error while trying to update timestream for {node_values['Device ID']} with {str(e)}")

logging.info("Started listening")
client = mqtt.Client("frizzle_receiver")
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("frizzle_test", "FRIZZLE")
client.connect(config['mqtt_server'], 1883, 60)
client.loop_forever()
