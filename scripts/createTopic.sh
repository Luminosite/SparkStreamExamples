#!/bin/sh
if [ $# -eq 3 ];then
	./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor $1 --partitions $2 --topic $3
else
	echo "createTopic [replication factor] [partition num] [topic]"
fi
