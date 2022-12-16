#!/usr/bin/env bash
#
#  ip-services-run.sh
#
cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh

spark-submit --master yarn \
  --deploy-mode cluster \
  --class $IPSERVICES_CLASS \
  target/$IPSERVICES_JAR \
  $@
