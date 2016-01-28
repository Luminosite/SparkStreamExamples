if [ $# -eq 1 ]
then
./kafka-topics.sh --describe --zookeeper localhost:2181 --topic $1
else
	echo "describe [topic]"
fi
