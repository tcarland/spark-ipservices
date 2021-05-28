#!/usr/bin/env bash
#
#  ip-services-run.sh
#
cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh

APP_CLASS="com.trace3.spark.IpServices"

spark-submit --master yarn \
  --class $IPSERVICES_CLASS \
  iptarget/$SERVICES_JAR \
  $@
