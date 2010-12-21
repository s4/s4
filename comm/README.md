S4 Communication Layer
======================

Introduction
------------
This is a component of the S4 streaming system. For more information, see [s4.io](http://s4.io)

Requirements
------------

* Linux
* Java 1.6
* Maven

Build Instructions
------------------

1. ZooKeeper must be installed to your local Maven repository manually. The jar is located at
   lib/zookeeper-3.1.1.jar within this project. To install, run the following command:
        mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=zookeeper -Dversion=3.1.1 -Dpackaging=jar -Dfile=lib/zookeeper-3.1.1.jar

2. Build and install using Maven
        mvn install
