# Frizzle Kafka

## This repository contains everything to do with the weather sensors

For the sake of simplicity, we will use the term **nodes** to refer to weather sensors 


The functions currently involved with the node are:

- Registration of the node
- Receiving data from the node

## Node registration
The folder structure is as follows:  
```
 app.py  
 get_data.py  
 database/
        __init__.py
        DynamodbHandler.py
```

1. app.py:  
This is the flask server that contains various routes that are required while registering the app.

    The routes are:  
    a. /node/create:  
    this generates a new ID for each node and generates a qr code corresponding to the generated id. This is called by the node during manifacturing  
    
    The route does not use anything from the request body
    returns an id in the format
    ```
    {"Device ID":<newly generated id>}
    ```

    b. /node/register:  
    This registers the given ID with the given location.

    The request body is as follows:
    ```
    {
        "Device ID":"123213_abh34j_njnjn",
        "lat":12.5971,
        "lng":77.6773
    }
    ``` 

    If it has successfully been registered, the response will be of the format  
    ```
    {
    "reason": "Node successfully registered",
    "status": "success"
    }
    ```

    if there is an error the resporse will be:
    ```
    {
    "reason": "reason mentioned here",
    "status": "failed"
    }
    ```
       

    c. /node/initialise:  
    this route is used by the node to get the current time required to sync the clock in the node
    In the body of the request has to be in the format below
    ```
    {
    "Device ID":"lorem ipsum"
    }
    ```

    And the response if the node HAS been REGISTERED is
    ```
    {
    "date": "2021-08-20",
    "time": "10:04:29.437049"
    }
    ```
     And when it is not registered
     ```
     {
    "reason": "id not present in ID database",
    "status": "failed"
    }
     ```


## Node data receiver
The folder directory is as follows
```
mqtt_receiver.py
config.json
mqtt_publish.py
```
1. mqtt_receiver.py
This file contains all the functions that are required to consume data from the mqtt broker

    -  def on_connect():
    this function is a callback that is triggered when we connect to the broker  

    - def on_message():
    This function is called when a message is received by the MQTT broker
    From the payload the data and the logs are separated and then pushed into different kafka topics, one for the data and one for the logs  

2. config.json
This file contains the configuration required for the mqtt receiver to run and contains all the topic names required to consume from and publish to

3. mqtt_publish.py
This file contains code that sends test data to the MQTT broker and is used for testing purposes

## General files
1. start_kafka_servers.sh 
This is used to start the single node kafka cluster onto the machine i.e. the zookeeper service and the kafka broker service

2. stop_kafka_servers.sh
This is used to stop the single node kafka cluster

3. start_servers.sh
This is used to start the linux services required for the Node registration i.e. nginx service and the registration service

4. stop_servers.sh
This is used to stop the linux services