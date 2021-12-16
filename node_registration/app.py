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
from qrcode import make
import io
from get_data import get_log
from timestream import create_table

#load config
with open("config.json","r") as f:
    config = loads(f.read())

db_handler = db.DynamodbHandler()
nodes_table = db_handler.db.Table(config['node_info'])
s3_resource = boto3.resource('s3')
qr_code_bucket = s3_resource.Bucket(config['qr_code_bucket'])
app = Flask(__name__)

logging.basicConfig(filename='node_server_requests.log', filemode="w", level=logging.INFO,format=config['log_format'])
app.logger.setLevel(logging.INFO)
timestream_client = boto3.client('timestream-write',region_name='us-east-1')


@app.route('/node/server/status', methods=["GET"])
def status():
    #this route tells if the server is reachable or not    
    app.logger.info(get_log(logging.INFO,request,None))
    return {"Server": "Node creation and registration", "Status": "Up"}

@app.route("/node/create", methods=["GET"])
def register_node():
    curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S")
    
    #generating the new ID for the node
    data = dict()
    data["Device ID"] = str(uuid.uuid4())
    
    #generating QR code for the ID    
    qr_code = make(data["Device ID"])
    date = datetime.now()
    tz = pytz.timezone('Asia/Kolkata')
    current_time = str(date.astimezone(tz))      

    #inserting into DynamoDB
    row = {"Device ID": {"S": data["Device ID"]},
           "registration": {"S": "incomplete"},
           "generation-time":{"S":current_time}}
    
    try:
        db_handler.client.put_item(
            TableName=config['node_info'],
            Item=row
        )
        log_desc = "Generated {id} at {time}".format(id=data["Device ID"],time=datetime.now().strftime(format="%y-%m-%d_%H-%M-%S"))
        app.logger.info(log_desc)
        
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)))
        return {"status": "failed", "reason": str(e)}
    
    #inserting the qr code into S3
    try:
        img = make(data['Device ID'])
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr,format="PNG")
        img_byte_arr = img_byte_arr.getvalue()

        qr_code_bucket.put_object(Body = img_byte_arr,
        Key=data["Device ID"]+datetime.now().strftime(format=" %y-%m-%d_%H-%M-%S")+".png",
        ServerSideEncryption='aws:kms',
        SSEKMSKeyId =config['KMSKeyID'])
        log_desc = "Put QR code with id: {id} at {time}".format(id=data["Device ID"],time=datetime.now().strftime(format="%y-%m-%d_%H-%M-%S"))
        app.logger.info(log_desc)
    
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)))
        return {"status": "failed", "reason": str(e)}
    
    return jsonify(data)


@app.route("/node/register",methods=["POST"])
def register():    
    
    try:
        node_info = loads(request.data)
    except Exception as e:
        # add logs here
        app.logger.error(get_log(logging.ERROR,request,str(e)))
        return jsonify({"parsing error ": str(e)})
    
    if node_info["Device ID"] == "":
        app.logger.error(get_log(logging.ERROR,request,"Empty ID in request"))
        return {'status': "failed", "reason": 'empty device id'}

    #checking if the node is registered or not
    response = db_handler.client.get_item(
        TableName=config['node_info'],
        Key={
            "Device ID": {
                "S": node_info['Device ID']
            }
        }
    )    
    try:
        # print("Checking if Id registered in DB",response['Item'])
        if response['Item'] == {}:
            app.logger.error(get_log(logging.error,request,"ID not present in database"))
            return {"status": "failed", "reason": "id not present in ID database"}
    except Exception as e:
        app.logger.error(get_log(logging.error,request,"ID not present in database"))
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
        log_desc = "{device} already present in database".format(device=node_info['Device ID'])
        app.logger.error(get_log(logging.error,request,log_desc))        
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
            TableName=config['node_info'],
            Item=row
        )
        log_desc = "Registered device with id {device} at {time}".format(device=node_info['Device ID'],time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S"))
        app.logger.info(log_desc)
    except Exception as e:
        log_desc = "Could not register device with id {device}".format(device=node_info['Device ID'])
        app.logger.error(log_desc)
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
    
    
    status = create_table(timestream_client,node_info['Device ID'])
    if status is False:
        return {"status": "failed", 'reason': 'error in updating registration'}
        
    return {"status":"success","reason":"Node successfully registered"}
    # add table to existing pool of device


@app.route("/node/initialise",methods=["POST"])
def initialise():
    try:
        node_info = loads(request.data)
    except Exception as e:
        app.logger.error(logging.ERROR,request,"unable to parse data")
        return jsonify({"parsing error ": str(e)})
    # print(node_info)
    if node_info["Device ID"] == "":
        app.logger.error(logging.ERROR,request,"Empty device ID")
        return {'status': "failed", "reason": 'empty device id'}

    response = db_handler.client.get_item(
        TableName=config['node_info'],
        Key={
            "Device ID": {
                "S": node_info['Device ID']
            }
        }
    )
    
    try:
        if response['Item']['registration']["S"] != "complete":
            log_desc = "{ID} not registered in database".format(ID=node_info["Device ID"])
            app.logger.error(logging.ERROR,request,log_desc)
            return {"status": "failed", "reason": "id not registered in ID database"}
    except Exception:
        log_desc = "{ID} not present in database".format(ID=node_info["Device ID"])
        app.logger.error(logging.ERROR,request,log_desc)
        return {"status": "failed", "reason": "id not present in ID database"}
    data = {}
    date = datetime.now()
    tz = pytz.timezone('Asia/Kolkata')
    current_time = str(date.astimezone(tz))    
    data['date'] = current_time.split()[0]
    data['time'] = current_time.split()[1].rstrip('+5:30')
    return jsonify(data)

if __name__ == "__main__":
    app.logger.setLevel(logging.INFO)
    app.run(debug=True, port=5000)
