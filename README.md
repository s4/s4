S4 - Stream Computing Platform
==============================

For more information, see [http://incubator.apache.org/s4/](http://incubator.apache.org/s4/)

Requirements
------------

* Linux
* Java 1.6

Build Instructions
------------------

* Build using Gradle

	- USAGE: gradlew TASK1, TASK2, ...
	
	- Main Tasks:
	
		+ clean: deletes all the build dirs
		+ jar: creates all the jars in PROJECT/build/libs
		+ binImage: creates an image in the ./build subdir that includes jars, 
		  scripts, and other resources 	required to run the S4 server and sample 
		  applications.
		+ binTgz: same in a gzipped tar file in ./build/distributions.
		+ allImage: creates an image in the ./build subdir that includes what is
		  in binImage plus sources, javadoc, and other documents.
		+ allTgz: same in a gzipped tar file in ./build/distributions.
		+ install: installs jars and POMs in local Maven repo (eg. ~/.m2)

The S4 Image Structure
----------------------

	build/s4-image/
                bin/
                s4-ext/
                s4-example-apps/
                s4-apps/
                        app-name1/
                                  lib/
                                      (app-name1-*.jar)
                                  app-name1-conf.xml
                s4-core/      
                        lib/
                        conf/
                        lock/
                        logs

                s4-driver/lib/
                               driver jar
                               dependencies jars
                               java examples jar
                          bin/
                               python, perl, and shell scripts



Running the Twitter Topic Count Example
---------------------------------------
<pre>
#  Download the project files from the repository.
git clone https://github.com/s4/s4.git

# Create image
./gradlew allImage

# set the S4_IMAGE environmental variable
cd build/s4-image/
export S4_IMAGE=`pwd`

# get the sample application
git clone git://github.com/s4/twittertopiccount.git

# build the sample application
./gradlew install

# deploy the sample application into the S4 image (relies in the S4_IMAGE environmental variable)
./gradlew deploy

# set the TWIT_LISTENER environmental variable
cd build/install/twitter_feed_listener
export TWIT_LISTENER=`pwd`

# Start server with twittertopiccount app
$S4_IMAGE/scripts/start-s4.sh -r client-adapter &

# start the client adapter
$S4_IMAGE/scripts/run-client-adapter.sh -s client-adapter -g s4 -d $S4_IMAGE/s4-core/conf/default/client-stub-conf.xml &

# run a client to send events into the S4 cluster. Replace *your-twitter-user* and *your-twitter-password* with your Twitter userid and password.
$TWIT_LISTENER/bin/twitter_feed_listener *your-twitter-user* *your-twitter-password* &

# Check output
cat /tmp/top_n_hashtags
</pre>

Developing with Eclipse
-----------------------

The command `gradle eclipse` will create an eclipse project that you can import from the Eclipse IDE.

There is now a [Gradle plugin for the Eclipse IDE](http://static.springsource.org/sts/docs/2.7.0.M1/reference/html/gradle/index.html). 
To install Gradle without installing the full Spring development environment follow the
[instructions](http://static.springsource.org/sts/docs/2.7.0.M1/reference/html/gradle/installation.html) under the heading 
"Installing from update site". There is also a discussion in the [Gradle mailing list](http://gradle.1045684.n5.nabble.com/ANN-Gradle-Eclipse-Plugin-td4387658.html).

