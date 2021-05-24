#!/bin/bash
#
#  ip-services-run.sh
#

SERVICES_JAR="ip-services-0.3.2-jar-with-dependencies.jar"

spark-submit --master yarn \
  --class com.trace3.spark.IpServices \
  target/$SERVICES_JAR \
  $@
