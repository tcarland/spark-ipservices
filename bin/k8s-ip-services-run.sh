#!/usr/bin/env bash
#

APP_JAR="s3a://spark/jars/ip-services-0.3.2-jar-with-dependencies.jar"
APP_CLASS="com.trace3.spark.IpServices"

( mc cp target/$APP_CLASS $MC/spark/jars )
if [ $? -ne 0 ]; then
    echo "Error in `mc cp`"
    exit 1
fi

sparkonk8s.sh ipservices $APP_CLASS $APP_JAR $@ )

