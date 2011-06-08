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

#---------------------------------------------
# USAGE and read arguments
#---------------------------------------------

if [ "$1" == "-h" ]; then
  echo "Usage: $0" >&2
  echo "  -c s4 core home" >&2
  echo "  -a adapter address" >&2
  echo "  -r emit rate" >&2
  echo "  -d rate display interval" >&2
  echo "  -s comma delimited list of schema files" >&2
  echo "  -h help" >&2
  exit 1
fi

BASE_DIR=`dirname $($READLINK -f $0)`
CORE_HOME=`$READLINK -f ${BASE_DIR}/../../s4-core`
LOAD_GENERATOR_HOME=`$READLINK -f ${BASE_DIR}/..`
CP_SEP=":"

while getopts ":c:a:r:d:l:" opt;
do  case "$opt" in
    c) CORE_HOME=$OPTARG;;
    a) ADAPTER_ADDRESS=$OPTARG;;
    r) RATE=$OPTARG;;
    d) DISPLAY_INTERVAL=$OPTARG;;
    l) LOCK_DIR=$OPTARG;;
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

INPUT_FILE=$1

if [ "x$ADAPTER_ADDRESS" == "x" ] ; then
    ADAPTER_ADDRESS="localhost:2334"
fi

if [ "x$RATE" == "x" ] ; then
    RATE=80
fi

if [ "x$DISPLAY_INTERVAL" == "x" ] ; then
    DISPLAY_INTERVAL=15
fi

if [ "x$LOCK_DIR" == "x" ] ; then
    LOCK_DIR="${CORE_HOME}/lock"
fi

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

JAVA_OPTS=""
if [ "x$LOCK_DIR" != "x" ] ; then
  JAVA_OPTS="$JAVA_OPTS -Dlock_dir=$LOCK_DIR "
fi

#echo "java location is ${JAVA_LOC}"
#echo -n "JAVA VERSION="
#echo `${JAVA_LOC}java -version`
#---------------------------------------------
#ADDING CORE JARS TO CLASSPATH
#---------------------------------------------

CLASSPATH=`find $CORE_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`
CLASSPATH=$CLASSPATH$CP_SEP`find $LOAD_GENERATOR_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`

CMD="${JAVA_LOC}java $JAVA_OPTS -classpath $CLASSPATH io.s4.tools.loadgenerator.LoadGenerator -a ${ADAPTER_ADDRESS} -r${RATE} -d ${DISPLAY_INTERVAL} $INPUT_FILE"
#echo "Running ${CMD}"
$CMD
