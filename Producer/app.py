import boto3
from flask import Flask, render_template, request
from flask_assets import Bundle, Environment
from datetime import datetime
from json import dumps, loads
from kafka import KafkaProducer
from kafka.cluster import ClusterMetadata
from kafka.admin import NewTopic
from flask.json import jsonify
from database import DynamodbHandler as db
import pytz
import uuid
producer = KafkaProducer(bootstrap_servers=['13.126.242.56:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

db_handler = db.DynamodbHandler()
nodes_table = db_handler.db.Table("Nodes_Available")
app = Flask(__name__)

@app.route('/')
def hello_world():
    if request.method == "GET":
        return 'Hello, World!'
    else:
        return "Root method uses only GET, Please try again"


@app.route("/register_node", methods=["POST"])
def register_node():
    if request.method == "POST":
        try:
            node_info = loads(request.data)
        except Exception as e:
            return jsonify({"parsing error ": str(e)})
        #print(node_info)
        data = dict()
        date = datetime.now()
        tz = pytz.timezone('Asia/Kolkata')
        current_time = str(date.astimezone(tz))
        current_time = str(date.astimezone(tz))
        data['date'] = current_time.split()[0]
        data['time'] = current_time.split()[1].rstrip('+5:30')
        if node_info["Device ID"] == "":
            device_id = str(uuid.uuid4())
            #print(topic_name)

            #creating table for the node
            status = db_handler.create_table_in_database(device_id)
            if status == "failed":
                data['error'] = 'dynamo failed'
                return jsonify(data)

	        #add table to existing pool of device
            try:
            	row = {"Device ID":device_id,"lat":"","lng":""}
            	with nodes_table.batch_writer() as writer:
                	writer.put_item(Item=row)
            except Exception as e:
            	print("could not insert into existing nodes")
                data['error'] = "could not update existing nodes' table"
            	return jsonify(data)

            data['id'] = device_id
            return jsonify(data)
        else:        
            try:
            	row = {"Device ID":node_info["Device ID"],"lat":node_info["lat"],"lng":node_info["lng"]}
            	with nodes_table.batch_writer() as writer:
                	writer.put_item(Item=row)
            except Exception as e:
            	print("could not insert location of nodes")
            	return jsonify(data)
            return jsonify(data)

    else:
        return {"Status": "Failed", "Reason": "Wrong method, POST method only"}


if __name__ == "__main__":
    app.run(debug=True, port=5000)
