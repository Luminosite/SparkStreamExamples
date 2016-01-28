if [ $# -eq 1 ]
then
	for i in $(seq $1)
	do
		./startOneMore.sh
	done
fi
