import paho.mqtt.client as mqtt

MQTT_server="65.1.190.134"
MQTT_path="testing_topic"

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
data = {"Name":"Rishith"}
client.connect(MQTT_server,1883)
client.publish(MQTT_path,str(data))
#  client.loop_forever()
