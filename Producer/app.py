import boto3
from flask import Flask, render_template, request
from flask_assets import Bundle, Environment
from datetime import datetime
from kafka import KafkaProducer
from json import dumps,loads
#producer = KafkaProducer(bootstrap_servers=['13.232.244.184:9092'],
#                        value_serializer=lambda x: 
#                       dumps(x).encode('utf-8'))
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
			
			# producer.send('node-3', value=node_sensor_values)
	    # print(node_sensor_values)
            # response_body = db_handler.insert(node_sensor_values)
			return {"Status": "Successful"}
		except Exception as e:
			return {"Status": "Failed", "reason": e}
	else:
		return "POST method only"

if __name__ == "__main__":
	app.run(debug=True)
