#!/usr/bin/env bash
~/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
--master local[2] \
--class com.paypal.risk.rds.KafkaStreamExample.KafkaStreamExampleMain \
~/IdeaProjects/SparkStreamExamples/target/scala-2.10/SparkStreamExamples_assembly_2.10.6-1.0.jar \
-kafkaOps horton-input-kafka.conf
-kafkaAPI--Direct
