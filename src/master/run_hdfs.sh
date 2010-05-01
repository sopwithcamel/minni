#!/bin/bash

LIB_HDFS="/home/wolf/Dropbox/CMU/hadoop-0.20.2/build/c++/Linux-amd64-64/lib"
LIB_JVM="/usr/lib/jvm/default-java/jre/lib/amd64/server"

HADOOP_CONF="home/wolf/Dropbox/CMU/hadoop-0.20.2/conf"
HADOOP_CORE="/home/wolf/Dropbox/CMU/hadoop-0.20.2/hadoop-0.20.2-core.jar"
LOG4J="/usr/share/java/commons-logging.jar"

CLASSPATH=$LOG4J:$HADOOP_CONF:$HADOOP_CORE:$CLASSPATH
LD_LIBRARY_PATH=$LIB_HDFS:$LIB_JVM:$LD_LIBRARY_PATH

export CLASSPATH
export LD_LIBRARY_PATH

$1
