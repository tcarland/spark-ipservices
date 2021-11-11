#!/usr/bin/env bash
#
# yarn-ipservices-run.sh
#
# spark-shell --jars delta-core_2.11-0.6.1.jar --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
#
cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh

APP_CLASS="com.trace3.spark.IpServices"

spark-submit --master yarn \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --class $IPSERVICES_CLASS \
  target/$IPSERVICES_JAR \
  $@
