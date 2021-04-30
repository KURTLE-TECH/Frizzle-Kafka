#!/bin/bash
cd $KAFKA_HOME
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > $HOME/nohup.out 2>&1 &
sleep 5
nohup bin/kafka-server-start.sh config/server.properties > $HOME/nohup.out 2>&1 &
sleep 5
