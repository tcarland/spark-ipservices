#!/usr/bin/env bash
#
#  ip-services-run.sh
#

APP_JAR="ip-services-0.3.2-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.IpServices"

spark-submit --master yarn \
  --class com.trace3.spark.IpServices \
  target/$SERVICES_JAR \
  $@
