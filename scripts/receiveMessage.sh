#!/bin/sh
if [ $# -eq 1 ] 
then
./kafka-console-consumer.sh --zookeeper localhost:2181 --topic $1 --from-beginning
else
echo "consumeMessage [topic]"
fi
