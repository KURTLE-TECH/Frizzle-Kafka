import paho.mqtt.client as mqtt

MQTT_server="13.126.242.56"
MQTT_path="Frizzle/Sensor_Data"

def on_connect(client,userdata,flags,rc):
    print("connected with code"+str(rc))

def on_message(client,userdata,msg):
    print("message received from "+str(msg.topic)+":"+str(msg.payload))

#client config
client = mqtt.Client("rishi_bhowmi_sender")
client.username_pw_set("frizzle_test","FRIZZLE")
client.on_connect = on_connect
client.on_message = on_message

#client connection and coms
data = {"Device ID":"643f2987-07c1-44e2-b7a1-ac9dad148d27","Temp":"23","Pressure":"0.9bar",'Humidity':'78','Wind speed':"25","wind direction":"S",'status':'working'}
client.connect(MQTT_server,1883)
client.publish(MQTT_path,str(data))
#  client.loop_forever()
