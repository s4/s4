CP_SEP=":"
CLASSPATH=`find ../ -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

CMD="${JAVA_LOC}java -classpath $CLASSPATH io.s4.client.example.Request $1 $2 $3 $4 $5"
echo $CMD
$CMD

