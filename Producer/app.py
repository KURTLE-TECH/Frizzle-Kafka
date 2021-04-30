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
producer = KafkaProducer(bootstrap_servers=['13.126.242.56:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

kafka_admin_client = KafkaAdminClient(bootstrap_servers=['13.126.242.56:9092'])
client = boto3.client('dynamodb')
dynamo_resource = boto3.resource('dynamodb')
nodes_table = dynamo_resource.Table("Nodes_Available")
app = Flask(__name__)
def create_table_in_database(client,topic_name):
    try:
        response = client.create_table(
            AttributeDefinitions=[
            {
            'AttributeName': 'time-stamp',
            'AttributeType': 'S'
            },
            ],
        TableName=topic_name,
        KeySchema=[
            {
                'AttributeName': 'time-stamp',
                'KeyType': 'HASH'
            },
        ],
        BillingMode='PAY_PER_REQUEST',
        StreamSpecification={
            'StreamEnabled': False,
        },
        SSESpecification={
            'Enabled': True ,
            'SSEType': 'KMS',
            'KMSMasterKeyId': '57780289-75ee-4f41-bdf8-0d4f43291fae'
        }
        )
        print(response)
    except Exception as e:
        print(e)

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
            print(date)
            topic_name = str(uuid.uuid4())
            print(topic_name)
            # creating topics for the node
            topics_list = list()
            topics_list.append(
                NewTopic(name=topic_name, num_partitions=1, replication_factor=3))
            try:
                kafka_admin_client.create_topics(new_topics=topics_list)
                print("inside topic creation")
            except Exception as e:
                data['error'] = e
                return jsonify(data)

            #creating table for the node
            status = create_table_in_database(client,topic_name)
            if status == "failed":
                data['error'] = 'dynamo failed'
                return jsonify(data)

	    #add table to existing pool of device
	    try:
            	row = {"Device ID":topic_name,"lat":"","lng":""}
            	with nodes_table.batch_writer() as writer:
                	writer.put_item(Item=row)
            except Exception as e:
            	print("could not insert into existing nodes")
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
            try:
            	row = {"Device ID":nodes_info["Device ID"],"lat":nodes_info["lat"],"lng":nodes_info["lng"]}
            	with nodes_table.batch_writer() as writer:
                	writer.put_item(Item=row)
            except Exception as e:
            	print("could not insert location of nodes")
            	return jsonify(data)
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
