#!/bin/bash

set -e -x

SPARK_MASTER_IP=`hostname`
SPARK_MASTER_PORT=7077
WEBUI_PORT=8080

DEPS_JARS="../assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4-deps.jar"
CORE_CLASSES="../core/target/scala-2.10/classes"

CP="$DEPS_JARS:$CORE_CLASSES"

export SPARK_HOME=/home/sen/git-store/spark

java -cp $CP org.apache.spark.deploy.worker.Worker spark://$IP:$PORT
