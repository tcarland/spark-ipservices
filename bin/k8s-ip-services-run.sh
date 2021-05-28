#!/usr/bin/env bash
#
MC="${MC:-callisto}"

cwd=$(dirname "$(readlink -f "$0")")
. $cwd/ipservices-config.sh

JAR_PATH="s3a://spark/jars"


( mc cp target/$IPSERVICES_JAR $MC/spark/jars/ )

if [ $? -ne 0 ]; then
    echo "Error in `mc cp`"
    exit 1
fi


sparkonk8s.sh ipservices $IPSERVICES_CLASS $JAR_PATH/$IPSERVICES_JAR $@ 

