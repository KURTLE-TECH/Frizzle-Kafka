import boto3
from flask import Flask, render_template, request
from flask_assets import Bundle, Environment
from datetime import datetime
from json import dumps, loads
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata
from kafka.admin import NewTopic
from flask.json import jsonify
from create_table import *
import pytz
import uuid
producer = KafkaProducer(bootstrap_servers=['13.126.242.56:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

client = KafkaAdminClient(bootstrap_servers=['13.126.242.56:9092'])
client = boto3.client('dynamodb')
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
        print(node_info)
        if node_info["Device ID"] == "":
            data = dict()
            date = datetime.now()
            tz = pytz.timezone('Asia/Kolkata')
            current_time = str(date.astimezone(tz))
            data['date'] = current_time.split()[0]
            data['time'] = current_time.split()[1].rstrip('+5:30')
            topic_name = str(uuid.uuid4())

            # creating topics for the node
            topics_list = list()
            topics_list.append(
                NewTopic(name=topic_name, num_partitions=1, replication_factor=3))
            try:
                client.create_topics(new_topics=topics_list)
            except Exception as e:
                data['error'] = e
                return jsonify(data)

            # creating the table for the node
            status = create_table_in_database(topic_name)
            if status == "failed":
                data['error'] = 'dynamo failed'
                return jsonify(data)

            data['id'] = topic_name
            return jsonify(data)
        else:
            data = dict()
            date = datetime.now()
            tz = pytz.timezone('Asia/Kolkata')
            current_time = str(date.astimezone(tz))
            current_time = str(date.astimezone(tz))
            data['date'] = current_time.split()[0]
            data['time'] = current_time.split()[1].rstrip('+5:30')
            return jsonify(data)

    else:
        return {"Status": "Failed", "Reason": "Wrong method, POST method only"}


@app.route("/show_topics", methods=["GET"])
def show_topics():
    try:
        return "working"
    except Exception as e:
        return dumps(str(e))


if __name__ == "__main__":
    app.run(debug=True, port=5000)
