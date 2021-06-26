#!/usr/bin/env bash
#
# sparkonk8s.sh -m 2g -w 4 IpServices com.trace3.spark.IpServices s3a://spark/jars/ip-services-0.3.2-jar-with-dependencies.jar s3a://scratch/services s3a://spark/ipservices
#
MC="${MC:-callisto}"
JAR_PATH="s3a://spark/jars"


cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh


( mc cp target/$IPSERVICES_JAR $MC/spark/jars/ )

if [ $? -ne 0 ]; then
    echo "Error in `mc cp`"
    exit 1
fi


if ! sparkonk8s.sh -e >/dev/null; then
    echo "Error with the spark run configuration."
    exit 1
fi

sparkonk8s.sh ipservices $IPSERVICES_CLASS $JAR_PATH/$IPSERVICES_JAR $@ 

exit $?