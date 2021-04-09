import boto3
from flask import Flask, render_template,request
from flask_assets import Bundle, Environment
from database import DynamodbHandler as db
from kafka import KafkaConsumer
from json import loads
import socket
import json

def send_reading(reading):
	with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
		s.connect(("localhost",5000))
		message=json.dumps(reading)
		s.send(message.encode())

consumer = KafkaConsumer('node-1',bootstrap_servers=["13.126.242.56:9092"],
	auto_offset_reset='latest',
     	enable_auto_commit=True,
	value_deserializer=lambda x:loads(x.decode('utf-8'))
	)
db_handler = db.DynamodbHandler("pes_node_1")
database_view=db_handler.view_database()
print(database_view)
for message in consumer:
	print(message.value)
	send_reading(message.value)
#	if not message.value:
	response_body = db_handler.insert(message.value)
	#database_view=db_handler.view_database()
	print(response_body)


