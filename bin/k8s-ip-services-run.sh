#!/usr/bin/env bash
#
# sparkonk8s.sh -m 2g -w 4 ipservices com.trace3.spark.IpServices s3a://spark/jars/ip-services-0.3.4-jar-with-dependencies.jar s3a://scratch/services s3a://spark/ipservices
#
MC="${MC:-callisto}"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh

jar_path="s3a://spark/jars"
app_class="com.trace3.spark.IpServicesTable"

( mc cp target/$IPSERVICES_JAR $MC/spark/jars/ )

if [ $? -ne 0 ]; then
    echo "Error in `mc cp`"
    exit 1
fi


if ! sparkonk8s.sh -e >/dev/null; then
    echo "Error with the spark run configuration."
    exit 1
fi

sparkonk8s.sh -H trino ipservices $app_class $jar_path/$IPSERVICES_JAR $@ 

exit $?