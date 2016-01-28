num=`ls -l scribeTmp/startServers/| grep "^-" |wc -l`
name="scribeTmp/startServers/server_${num}.properties"
echo "broker.id=$num" > $name
port=$[$num+9092]
echo "port=$port" >> $name
echo "zookeeper.connect=localhost:2181" >> $name
echo "log.dir=/tmp/kafka-logs-$num" >> $name
./kafka-server-start.sh $name &

