#!/bin/bash

HOME="/usr/local/kafka"
TOPIC="hadoop_data"

gnome-terminal -- bash -c "$HOME/bin/kafka-server-start.sh $HOME/config/server.properties"

kafka-topic.sh --bootstrap-server localhost:9092 --topic $TOPIC --create

gnome-terminal -- bash -c "$HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC"
