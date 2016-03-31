#!/usr/bin/env bash

#-----local spark-----

readonly submit=/hadoop/home/hadoop/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit

#-----spark configure-----

readonly SPARK_MASTER="yarn-client"
readonly HORTON_QUEUE="pp_risk_dst"
readonly EXECUTOR_MEMORY="1G"
readonly NUM_EXECUTORS=144

#-----program info-----

readonly jars=SparkStreamExamples_assembly_2.10.6-1.0.jar
readonly HORTON_KAFKA_CONF=../conf/horton-input-kafka.conf
readonly KAFKA_API=-kafkaAPI--Direct #-kafkaAPI--Receiver
readonly INTERVAL=2
readonly PERIOD=120

#-----program info-----

readonly HBase_TTL=9

