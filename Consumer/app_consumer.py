import boto3
from flask import Flask, render_template,request
from flask_assets import Bundle, Environment
from database import DynamodbHandler as db
from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer('node-1',bootstrap_servers=["13.126.242.56:9092"],
	auto_offset_reset='latest',
     	enable_auto_commit=True,
	value_deserializer=lambda x:loads(x.decode('utf-8'))
	)
db_handler = db.DynamodbHandler("pes_node_1")
for message in consumer:
	print(message.value)
#	if not message.value:
	response_body = db_handler.insert(message.value)
	print(response_body)
