#!/usr/bin/env bash
~/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
--master local[8] --class priv.Luminosite.KafkaCompare.KafkaCompareMain \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0,org.apache.hbase:hbase-common:0.98.4-hadoop2,org.apache.hbase:hbase-server:0.98.4-hadoop2 \
~/IdeaProjects/SparkStreamExamples/target/scala-2.10/sparkstreamexamples_2.10-1.0.jar $@
