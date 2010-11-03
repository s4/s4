
if [ "x$1" == "x" ]; then
    echo "No target image directory specified"
    exit 1
fi

rm -fr $1/s4_apps/twittertopiccount
rm -fr $1/s4_apps/twittertopiccount/lib
rm -fr $1/s4_apps/twittertopiccount/schemas

mkdir $1/s4_apps/twittertopiccount
mkdir $1/s4_apps/twittertopiccount/lib
mkdir $1/s4_apps/twittertopiccount/schemas

cp ../../twittertopiccount/target/twittertopiccount-*.jar $1/s4_apps/twittertopiccount/lib
cp ../../twittertopiccount/target/twittertopiccount-*.dir/twittertopiccount_conf.xml $1/s4_apps/twittertopiccount/
cp ../../twittertopiccount/target/twittertopiccount-*.dir/adapter_conf.xml $1/s4_apps/twittertopiccount/
cp ../../twittertopiccount/target/twittertopiccount-*.dir/lib/commons-httpclient-*.jar $1/s4_apps/twittertopiccount/lib
cp ../../twittertopiccount/target/twittertopiccount-*.dir/lib/commons-codec-*.jar $1/s4_apps/twittertopiccount/lib
