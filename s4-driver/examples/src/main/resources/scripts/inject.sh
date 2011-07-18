#!/bin/bash

osx=false
case "`uname`" in
Darwin*) osx=true;;
esac

if $osx; then
    READLINK="stat"    
else
    READLINK="readlink"
fi

BASE_DIR=`dirname $($READLINK -f $0)`
INJECT_HOME=`$READLINK -f ${BASE_DIR}/..`

CP_SEP=":"
CLASSPATH=`find ${INJECT_HOME} -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

CMD="${JAVA_LOC}java -classpath $CLASSPATH io.s4.client.example.Inject $1 $2 $3 $4"
#echo $CMD
$CMD

