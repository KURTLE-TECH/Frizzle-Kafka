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
import pytz
import uuid
# producer = KafkaProducer(bootstrap_servers=['13.232.244.184:9092'],
#                        value_serializer=lambda x:
#                       dumps(x).encode('utf-8'))
client = KafkaAdminClient(bootstrap_servers=['13.126.242.56:9092'])
app = Flask(__name__)


@app.route('/')
def hello_world():
    if request.method == "GET":
        return 'Hello, World!'
    else:
        return "Root method uses only GET, Please try again"


@app.route("/push_to_queue", methods=['POST'])
def push_to_queue():
    if request.method == "POST":
        try:
            node_sensor_values = loads(request.data)
            node_sensor_values['time-stamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
            # producer.send('node-1', value=node_sensor_values)
			# print(node_sensor_values)
			# response_body = db_handler.insert(node_sensor_values)
            return {"Status": "Successful","Current":"Just accepting request only"}
        except Exception as e:
            return {"Status": "Failed", "reason": e}
    else:
        return "POST method only"

@app.route("/register_node",methods=["POST"])
def register_node():
    if request.method=="POST":
        try:
            node_info =loads(request.data)
        except Exception as e:
            # print(e)
            return jsonify({"parsing error ":str(e)})
        print(node_info)
        if node_info["Device ID"] == "":
            data = dict()
            date = datetime.now()
            tz = pytz.timezone('Asia/Kolkata')
            current_time = str(date.astimezone(tz))
            data['date'] = current_time.split()[0]
            data['time'] = current_time.split()[1].rstrip('+5:30')
            topic_name = str(uuid.uuid4())
            
            topics_list = list()
            topics_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=3))
            
            try:
            	client.create_topics(new_topics = topics_list)
            except Exception as e:
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
        return {"Status":"Failed","Reason":"Wrong method, POST method only"}

@app.route("/show_topics",methods=["GET"])
def show_topics():
    try:
        # connection = ClusterMetadata(bootstrap_servers=['13.126.242.56:9092'])
        # print(connection.topics())
        return "working"
    except Exception as e:
        return dumps(str(e))


if __name__ == "__main__":
    app.run(debug=True,port=5000)

