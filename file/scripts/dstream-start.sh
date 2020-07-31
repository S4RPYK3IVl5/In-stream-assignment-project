#!/bin/bash
 spark-submit --class dstream --packages \
com.redislabs:spark-redis_2.11:2.4.2,net.debasishg:redisclient_2.11:3.30,\
org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.4,\
com.datastax.spark:spark-cassandra-connector_2.11:2.5.0 \
../../target/scala-2.11/in-stream-assignment-project_2.11-0.1.jar