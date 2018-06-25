#!/bin/bash
#

SCALA_FILES="src/test/scala/ProtoTest.scala"
SCALA_CLASS="ProtoTest"

( mkdir -p target; scalac -d target $SCALA_FILES )

r=$?

if [ $r -eq 0 ]; then
    ( scala -cp target $SCALA_CLASS $@ )
    r=$?
else
    echo "Error in compile"
fi

exit $r
