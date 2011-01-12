Speech01 in Scala
=================

Introduction
------------
This is a rewrite of the (hello world-ish) [speech01](http://docs.s4.io/manual/getting_events_into_s4.html) application in the [Scala](http://www.scala-lang.org/) programming language. 

Requirements
------------

* Linux
* Java 1.6
* Maven
* S4 Communication Layer
* S4 Core

Build Instructions
------------------

1. First build and install the comm and core packages in your Maven repository.

2. Build and install using Maven

        mvn install assembly:assembly

Run Instructions
------------------

Follow instructions for the [speech01](http://docs.s4.io/manual/getting_events_into_s4.html#building-and-running-the-speech01-example) example in Java. The only change is in the command for piping events into the application. Do the following: 

    head -10 ${SOURCE_BASE}/examples/testinput/speeches.txt | \
    ./generate_load.sh -x -r 2 -u ../s4_apps/speech01_scala/lib/\* -

 

