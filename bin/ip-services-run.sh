#!/bin/bash
#
#  ip-services-run.sh
#
#  packages will still check remote regardless of local cache,
#  but jars will not.
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
#
#  KAFKA_JAR="$HOME/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/spark-sql-kafka-0-10_2.11-2.2.0.jar"
#  --jars $KAFKA_JAR \
#

SERVICES_JAR="ip-services-0.1.5-jar-with-dependencies.jar"

spark-submit --master yarn \
  --class com.trace3.spark.IpServices \
  target/$SERVICES_JAR \
  $@
