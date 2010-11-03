
if [ "x$1" == "x" ]; then
    echo "No target image directory specified"
    exit 1
fi

rm -fr $1/s4_exts/persister
rm -fr $1/s4_apps/twittertopiccount
rm -fr $1/s4_apps/twittertopiccount/lib
rm -fr $1/s4_apps/twittertopiccount/schemas
rm -fr $1/s4_apps/clickviewcount
rm -fr $1/s4_apps/clickviewcount/lib
rm -fr $1/s4_apps/clickviewcount/schemas

mkdir $1/s4_exts/persister
mkdir $1/s4_apps/twittertopiccount
mkdir $1/s4_apps/twittertopiccount/lib
mkdir $1/s4_apps/twittertopiccount/schemas
mkdir $1/s4_apps/clickviewcount
mkdir $1/s4_apps/clickviewcount/lib
mkdir $1/s4_apps/clickviewcount/schemas

cp ../../persister/target/persister-*.dir/persister_conf.xml $1/s4_exts/persister/
cp ../../clickviewcount/target/clickviewcount-*.jar $1/s4_apps/clickviewcount/lib
cp ../../clickviewcount/target/clickviewcount-*.dir/schemas/*_schema.js $1/s4_apps/clickviewcount/schemas
cp ../../clickviewcount/target/clickviewcount-*.dir/clickviewcount_conf.xml $1/s4_apps/clickviewcount/

cp ../../twittertopiccount/target/twittertopiccount-*.jar $1/s4_apps/twittertopiccount/lib
cp ../../twittertopiccount/target/twittertopiccount-*.dir/twittertopiccount_conf.xml $1/s4_apps/twittertopiccount/
cp ../../twittertopiccount/target/twittertopiccount-*.dir/adapter_conf.xml $1/s4_apps/twittertopiccount/
cp ../../twittertopiccount/target/twittertopiccount-*.dir/lib/commons-httpclient-*.jar $1/s4_apps/twittertopiccount/lib
cp ../../twittertopiccount/target/twittertopiccount-*.dir/lib/commons-codec-*.jar $1/s4_apps/twittertopiccount/lib
