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
data = {"Device ID":"519eb77c-98ea-4b76-a017-5ff4abfe0e56","Temp":"23","Pressure":"0.9bar",'status':'working'}
client.connect(MQTT_server,1883)
client.publish(MQTT_path,str(data))
#  client.loop_forever()
