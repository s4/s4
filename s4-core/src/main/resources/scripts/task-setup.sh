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
CORE_HOME=`$READLINK -f ${BASE_DIR}/../s4-core`
CP_SEP=":"

while getopts ":c:" opt;
do  case "$opt" in
    c) CORE_HOME=$OPTARG;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
     :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    esac
done
shift $(($OPTIND-1))

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

CP=`find ${CORE_HOME}/lib -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`

cmd="${JAVA_LOC}java -classpath $CP org.apache.s4.comm.tools.TaskSetupApp $*"
echo "RUNNING $cmd"
$cmd
