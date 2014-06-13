#!/bin/bash

set -e -x

IP="127.0.0.1"
PORT="7077"
WEB_PORT="8080"

DEPS_JARS="../assembly/target/scala-2.10/spark-assembly-1.1.0-SNAPSHOT-hadoop1.0.4-deps.jar"
CORE_CLASSES="../core/target/scala-2.10/classes"
EXAMPLES_CLASSES="../examples/target/scala-2.10/classes"
REPL_CLASSES="../repl/target/scala-2.10/classes"
TOOLS_CLASSES="../tools/target/scala-2.10/classes"

CP="$DEPS_JARS:$CORE_CLASSES"

export SPARK_HOME=/home/sen/git-store/spark

java -cp $CP org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7077
