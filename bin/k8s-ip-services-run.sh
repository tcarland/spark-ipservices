#!/usr/bin/env bash
#
MC="${MC:-callisto}"

APP_JAR="ip-services-0.3.2-jar-with-dependencies.jar"
APP_JAR_PATH="s3a://spark/jars"
APP_CLASS="com.trace3.spark.IpServices"

( mc cp target/$APP_JAR $MC/spark/jars/ )
if [ $? -ne 0 ]; then
    echo "Error in `mc cp`"
    exit 1
fi

sparkonk8s.sh ipservices $APP_CLASS $APP_JAR_PATH/$APP_JAR $@ 

