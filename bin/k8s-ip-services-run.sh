#!/usr/bin/env bash
#
# sparkonk8s.sh -m 2g -w 4 ipservicenames com.trace3.spark.IpServiceNames s3a://spark/jars/ip-service-names-0.4.0-jar-with-dependencies.jar s3a://scratch/services s3a://spark/ipservices
#
MC="${MC:-localhost}"
HIVE_NS="${HIVE_NAMESPACE:-trino}"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh

jar_path="s3a://spark/jars"
app_class="com.trace3.spark.IpServiceNamesTable"

# copy jar
jar=$(mc ls $MC/spark/jars/$IPSERVICES_JAR 2>/dev/null)
if [ -z "$jar" ]; then
    ( mc cp target/$IPSERVICES_JAR $MC/spark/jars/ )
fi 
if [ $? -ne 0 ]; then
    echo "Error in `mc cp`"
    exit 1
fi

if ! sparkonk8s.sh -e >/dev/null; then
    echo "Error with the spark run configuration."
    exit 1
fi

sparkonk8s.sh -H $HIVE_NS ipservicenames $app_class $jar_path/$IPSERVICES_JAR $@ 

exit $?