import boto3
from flask import Flask, render_template,request
from flask_assets import Bundle, Environment
from database import DynamodbHandler as db
from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer('pes_node_2')
db_handler = db.DynamodbHandler("pes_node_2")        
try:
        for message in consumer:            
            print(loads(message))
            node_sensor_values = loads(message)
            response_body = db_handler.insert(node_sensor_values)

except Exception as e:            
        print({"Status":"Failed","reason":e})
