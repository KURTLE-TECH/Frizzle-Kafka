#!/bin/bash
cd $KAFKA_HOME
nohup bin/zookeeper-server-start.sh -daemon config/zookeeper.properties > $HOME/nohup.out 2>&1 &
sleep 2
nohup bin/kafka-server-start.sh -daemon config/server.properties > $HOME/nohup.out 2>&1 &
sleep 2
