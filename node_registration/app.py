import json
import boto3
from flask import Flask, render_template, request
from flask_assets import Bundle, Environment
from datetime import date, datetime
from json import dumps, loads
from flask.json import jsonify
from database import DynamodbHandler as db
import pytz
import uuid
import logging

db_handler = db.DynamodbHandler()
nodes_table = db_handler.db.Table("Nodes_Available")
app = Flask(__name__)

# logging.basicConfig(filename='server_nodes.log', filemode="w", level=logging.DEBUG)
# formatter = logging.Formatter("Level:%(levelname)s %(name)s : %(message)s")
# handler = logging.FileHandler("requests_nodes.log", mode="w")
# handler.setFormatter(formatter)
# app_logger = logging.getLogger("requests")
# app_logger.setLevel(logging.INFO)
# app_logger.addHandler(handler)


@app.route('/status/server/nodes', methods=["GET"])
def status():
    return {"Server": "Node creation and registration", "Status": "Up"}


@app.route("/create", methods=["GET"])
def register_node():
    data = dict()
    data["Device ID"] = str(uuid.uuid4())
    date = datetime.now()
    tz = pytz.timezone('Asia/Kolkata')
    current_time = str(date.astimezone(tz))      
    row = {"Device ID": {"S": data["Device ID"]},
           "registration": {"S": "incomplete"},
           "generation-time":{"S":current_time}}
    try:
        db_handler.client.put_item(
            TableName="Nodes_Available",
            Item=row
        )
    except Exception as e:
        print(e)
        return {"status": "failed", "reason": str(e)}
    return jsonify(data)


@app.route("/register",methods=["POST"])
def register():
    try:
        node_info = loads(request.data)
    except Exception as e:
        # add logs here
        return jsonify({"parsing error ": str(e)})
    # print(node_info)
    if node_info["Device ID"] == "":
        return {'status': "failed", "reason": 'empty device id'}

    response = db_handler.client.get_item(
        TableName='Nodes_Available',
        Key={
            "Device ID": {
                "S": node_info['Device ID']
            }
        }
    )    
    try:
        print("Checking if Id registered in DB",response['Item'])
        if response['Item'] == {}:
            return {"status": "failed", "reason": "id not present in ID database"}
    except Exception as e:
        return {"status": "failed", "reason": "id not present in ID database"}
    data = {}
    date = datetime.now()
    tz = pytz.timezone('Asia/Kolkata')
    current_time = str(date.astimezone(tz))    
    data['date'] = current_time.split()[0]
    data['time'] = current_time.split()[1].rstrip('+5:30')

    try:
        sensor_values_creation = db_handler.create_table_in_database(node_info['Device ID'])
        sensor_health_creation = db_handler.create_table_in_database(node_info['Device ID']+"_logs")
    except Exception as e:
        return {"status":'failed','reason':'node already registered; tables exist'}    
    
    row = {"Device ID": {"S":node_info["Device ID"]},
            "lat": {"S":node_info["lat"]},
            "lng": {"S":node_info["lng"]},
            "registration":{"S":"complete"},
            "time-of-registration":{"S":current_time},
            "generation-time":{"S":response["Item"]["generation-time"]["S"]}
            }
    try:
        table_insertion_status = db_handler.client.put_item(
            TableName="Nodes_Available",
            Item=row
        )
        print("After updating registration to complete",table_insertion_status)
    except Exception as e:
        print("could not insert location of nodes")
        try:
            response = db_handler.client.delete_table(
                TableName=node_info['Device ID']+"_logs"
                )
            response = db_handler.client.delete_table(
                TableName=node_info['Device ID']
                )
        except Exception:
            pass
        return {"status": "failed", 'reason': 'error in updating registration'}
    return {"status":"success","reason":"Node successfully registered"}
    # add table to existing pool of device


@app.route("/initialise",methods=["GET"])
def initialise():
    try:
        node_info = loads(request.data)
    except Exception as e:
        # add logs here
        return jsonify({"parsing error ": str(e)})
    # print(node_info)
    if node_info["Device ID"] == "":
        return {'status': "failed", "reason": 'empty device id'}

    response = db_handler.client.get_item(
        TableName='Nodes_Available',
        Key={
            "Device ID": {
                "S": node_info['Device ID']
            }
        }
    )
    
    try:
        if response['Item']['registration']["S"] != "complete":
            return {"status": "failed", "reason": "id not registered in ID database"}
    except Exception:
        return {"status": "failed", "reason": "id not present in ID database"}
    data = {}
    date = datetime.now()
    tz = pytz.timezone('Asia/Kolkata')
    current_time = str(date.astimezone(tz))    
    data['date'] = current_time.split()[0]
    data['time'] = current_time.split()[1].rstrip('+5:30')
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
