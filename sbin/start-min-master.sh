#!/bin/bash

set -e -x

sbin=`dirname $0`
sbin=`cd $sbin; pwd`

SPARK_MASTER_IP=`hostname`
SPARK_MASTER_PORT=7077
WEBUI_PORT=8080

DEPS_JARS="$sbin/../assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4-deps.jar"
CORE_CLASSES="$sbin/../core/target/scala-2.10/classes"

CP="$DEPS_JARS:$CORE_CLASSES"

java -cp $CP org.apache.spark.deploy.master.Master --ip $IP --port $PORT --webui-port $WEB_PORT
