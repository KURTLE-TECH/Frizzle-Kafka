from kafka import KafkaProducer
from json import dumps
from datetime import datetime
from time import sleep
#13.232.244.184
producer = KafkaProducer(bootstrap_servers=['13.232.244.184:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for i in range(10):
	sleep(1)
	producer.send('node-3', value={"time-stamp":datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),"humidity":str(i)})
print("Sent")
