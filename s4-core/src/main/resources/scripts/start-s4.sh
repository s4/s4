#!/bin/bash

get_property()
{
  val=`sed '/^\#/d' ${CONF_LOC}/s4\-core.properties\-header | grep $1  | tail -n 1 | sed 's/^[^=]*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`
  echo "$val"
}

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
  echo "  -a s4 applications home" >&2
  echo "  -d s4 clock type" >&2
  echo "  -e s4 extensions home" >&2
  echo "  -l communications layer lock file base directory" >&2
  echo "  -i instance id (for log file names)" >&2
  echo "  -z cluster manager address (hostname:port)" >&2
  echo "  -r adapter cluster name" >&2
  echo "  -g cluster name" >&2
  echo "  -h help" >&2
  echo "  -s seed time (event clock initialization time)" >&2
  exit 1
fi
BASE_DIR=`dirname $($READLINK -f $0)`
CORE_HOME=`$READLINK -f ${BASE_DIR}/../s4-core`
APPS_HOME=`$READLINK -f ${BASE_DIR}/../s4-apps`
EXTS_HOME=`$READLINK -f ${BASE_DIR}/../s4-exts`
S4_CLOCK="wall"
#SEED_TIME="1234567890"
while getopts ":c:a:d:i:z:l:g:e:r:s:j:" opt;
do  case "$opt" in
    c) CORE_HOME=$OPTARG;;
    a) APPS_HOME=$OPTARG;;
    d) S4_CLOCK=$OPTARG;;
    e) EXTS_HOME=$OPTARG;;
    i) INSTANCE_ID=$OPTARG;;
    l) LOCK_DIR=$OPTARG;;
    z) CLUSTER_MANAGER=$OPTARG;;
    g) CLUSTER_NAME=$OPTARG;;
    r) ADAPTER_CLUSTER_NAME=$OPTARG;;
    s) SEED_TIME=$OPTARG;;
    j) EXTRA_JAVA_OPTS=$OPTARG;;
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

CONF_TYPE=$1
if [ "x$CONF_TYPE" == "x" ] ; then
    CONF_TYPE="default"
fi

CONF_FILE=${CORE_HOME}"/conf/"${CONF_TYPE}"/s4-core-conf.xml"
CONF_LOC=`dirname $CONF_FILE`
LOG_LOC="${CORE_HOME}/logs"

REMOTE_DEBUG_ENABLED="no"
DEBUG_OPTS=""
JAVA_OPTS=""
S4_OPTS=""

if [ "x$CLUSTER_MANAGER" == "x" ] ; then
    CLUSTER_MANAGER="localhost:2181"
fi
if [ "x$CLUSTER_NAME" == "x" ] ; then
    CLUSTER_NAME="s4"
fi
if [ "x$ADAPTER_CLUSTER_NAME" == "x" ] ; then
    ADAPTER_CLUSTER_NAME=$CLUSTER_NAME
fi
if [ "x$LOCK_DIR" == "x" ] ; then
    LOCK_DIR="${CORE_HOME}/lock"
fi
if [ "x$SEED_TIME" != "x" ] ; then
    S4_OPTS="-s ${SEED_TIME}"
fi

MKTEMP_ARGS=""

if $osx ; then
    MKTEMP_ARGS="tmpXXXX" 
fi

TMP1=`mktemp -d $MKTEMP_ARGS`
echo "Temp is $TMP1"

cat $CONF_LOC/s4-core.properties-header > $TMP1/s4-core.properties

echo "zk_address=${CLUSTER_MANAGER}" >> $TMP1/s4-core.properties
echo "commlayer_mode=dynamic" >> $TMP1/s4-core.properties
echo "s4_app_name=${CLUSTER_NAME}" >> $TMP1/s4-core.properties
echo "adapter_app_name=${ADAPTER_CLUSTER_NAME}" >> $TMP1/s4-core.properties

#---------------------------------------------
#Setting Prefix and classpath separator to handle windows:-)
#---------------------------------------------
CP_SEP=":"
PREFIX=""

#---------------------------------------------
# Setting important environment variables
#---------------------------------------------
MEM_OPTS=$(get_property "mem_opts")
GC_OPTS=$(get_property "gc_opts")
REMOTE_DEBUG_ENABLED=$(get_property "remote_debug_enabled")
COMMLAYER_MODE=$(get_property "commlayer_mode")
ZK_SESSION_TIMEOUT=$(get_property "zk_session_timeout")

if [ "x$GC_OPTS" == "x" ] ; then
  GC_OPTS=" -server -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit"
fi
if [ "x$MEM_OPTS" == "x" ] ; then
  MEM_OPTS="-Xms800m -Xmx2000m"
fi
if [ "$REMOTE_DEBUG_ENABLED" == "yes" ]; then
  DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n"
  DEBUG_SLEEP_TIME=15000
fi
if [ "x$COMMLAYER_MODE" != "x" ] ; then
  JAVA_OPTS="$JAVA_OPTS -Dcommlayer.mode=$COMMLAYER_MODE "  
fi
if [ "x$ZK_SESSION_TIMEOUT" != "x" ] ; then
  JAVA_OPTS="$JAVA_OPTS -Dzk.session.timeout=$ZK_SESSION_TIMEOUT "  
fi
if [ "x$LOCK_DIR" != "x" ] ; then
  JAVA_OPTS="$JAVA_OPTS -Dlock_dir=$LOCK_DIR "
fi

if [ "x$EXTRA_JAVA_OPTS" != "x" ] ; then
  BLIP=`eval echo ${EXTRA_JAVA_OPTS}`
  JAVA_OPTS="$JAVA_OPTS $BLIP"
fi

JAVA_OPTS="$JAVA_OPTS -Dlog_loc=${LOG_LOC} "

echo "CORE_HOME='$CORE_HOME'"
echo "APPS_HOME='$APPS_HOME'"
echo "EXTS_HOME='$EXTS_HOME'"
echo "GC_OPTS='$GC_OPTS'"
echo "MEM_OPTS='$MEM_OPTS'"
echo "JAVA_OPTS='$JAVA_OPTS'"
# figure out the location of the java command

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

echo "java location is ${JAVA_LOC}"
echo -n "JAVA VERSION="
echo `${JAVA_LOC}java -version`
#---------------------------------------------
#ADDING EXTENSIONS AND APPS JARS TO CLASSPATH
#---------------------------------------------

CLASSPATH=`find ${CORE_HOME}/lib -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`
CLASSPATH=$CLASSPATH$CP_SEP`find $APPS_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`
CLASSPATH=$CLASSPATH$CP_SEP`find $EXTS_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`

for i in `ls $APPS_HOME`
do
  CLASSPATH=$CLASSPATH$CP_SEP$APPS_HOME/$i
done

for i in `ls $EXTS_HOME`
do
  CLASSPATH=$CLASSPATH$CP_SEP$EXTS_HOME/$i
done

CLASSPATH=$CLASSPATH$CP_SEP$TMP1$CP_SEP$CONF_LOC

#---------------------------------------------
#STARTING S4 
#---------------------------------------------

CMD="${JAVA_LOC}java $GC_OPTS $DEBUG_OPTS $MEM_OPTS $JAVA_OPTS -classpath $CORE_HOME$CP_SEP$CLASSPATH -DDequeuerCount=6 -Dlog4j.configuration=file:${CONF_LOC}/log4j.xml io.s4.MainApp -c ${CORE_HOME} -a ${APPS_HOME} -e ${EXTS_HOME} -t ${CONF_TYPE} -d ${S4_CLOCK} $S4_OPTS"
echo "RUNNING $CMD"

exec ${CMD}
