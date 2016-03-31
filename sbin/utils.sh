#!/usr/bin/env bash
readonly _sbin="$( cd "$( dirname $0 )" && pwd )"
readonly root_dir=`dirname ${_sbin}`

submit_job() {
	local queue=$1; shift;
	${submit} \
	--master ${SPARK_MASTER} \
	--class com.paypal.risk.rds.KafkaStreamExample.KafkaStreamExampleMain \
	--executor-memory ${EXECUTOR_MEMORY} \
	--num-executors ${NUM_EXECUTORS} \
	--queue ${queue} \
	$root_dir/bin/${jars} \
	-kafkaOps $root_dir/conf/horton-input-kafka.conf \
	-kafkaAPI--Direct \
	-hbaseOps $root_dir/conf/hbase_config.conf \
	-interval ${INTERVAL} \
	-period ${PERIOD} $@

}
# submit job to local node
local_job_submitx() {
echo $@
}
local_job_submit() {
	${submit} \
	--master local[2] \
	--class com.paypal.risk.rds.KafkaStreamExample.KafkaStreamExampleMain \
	$root_dir/bin/${jars} \
	-kafkaOps $root_dir/conf/horton-input-kafka.conf \
	-kafkaAPI--Direct \
	-hbaseOps $root_dir/conf/hbase_config.conf \
	-interval ${INTERVAL} \
	-period ${PERIOD} $@
}

horton_job_submit() {
	submit_job "${HORTON_QUEUE}" $@
}
