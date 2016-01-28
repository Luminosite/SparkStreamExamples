#!/bin/sh
if [ $# -eq 2 ]
then
 ./kafka-console-producer.sh --broker-list $1 --topic $2
else
	echo "sendMessage [broker addr] [topic]"
fi
