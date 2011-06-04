S4 - Stream Computing Platform
==============================

For more information, see [s4.io](http://s4.io)

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
gradlew allImage

# Change permissions
# (Until this Gradle bug is fixed: http://issues.gradle.org/browse/GRADLE-796)
chmod u+x ./build/s4-image/scripts/*

# Copy S4 application to deployment dir (s4-apps)
cp -rp build/s4-image/s4-example-apps/s4-example-twittertopiccount build/s4-image/s4-apps/

# Enter your twitter user/pass in config file
$EDITOR build/s4-image/s4-apps/s4-example-twittertopiccount/adapter-conf.xml 

# Start server with s4-example-twittertopiccount app
./build/s4-image/scripts/start-s4.sh &

# Start adapter
 ./build/s4-image/scripts/run-adapter.sh -x -u build/s4-image/s4-apps/s4-example-twittertopiccount/lib/s4-example-twittertopiccount-0.3-SNAPSHOT.jar -d build/s4-image/s4-apps/s4-example-twittertopiccount/adapter-conf.xml &

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

