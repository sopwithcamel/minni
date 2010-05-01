JAVA_HOME=/usr/lib/jvm/default-java
HADOOP_HOME=/home/wolf/Dropbox/CMU/hadoop-0.20.2

INCLUDE_THRIFT=/usr/local/include/thrift
INCLUDE_HADOOP=$(HADOOP_HOME)/src/c++/libhdfs
INCLUDE_JAVA=$(JAVA_HOME)/include
INCLUDE_LINUX_JAVA=$(JAVA_INCLUDE)/linux

LIB_HDFS=$(HADOOP_HOME)/build/c++/Linux-amd64-64/lib
LIB_JAVA=/usr/lib/jvm/default-java/jre/lib/amd64/server

LINK_THRIFT=thrift
LINK_HDFS=hdfs
LINK_JVM=jvm

INCLUDE=-I$(INCLUDE_THRIFT) -I$(INCLUDE_HADOOP) -I$(INCLUDE_JAVA) -I$(INCLUDE_LINUX_JAVA)
LIB=-L$(LIB_JAVA) -L$(LIB_HDFS)
LINK=-l$(LINK_THRIFT) -l$(LINK_HDFS) -l$(LINK_JVM)

CFLAGS=-Wall $(INCLUDE) $(LIB)  $(LINK)

all:
	g++ $(CFLAGS) gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Master.cpp gen-cpp/Master_Server.cpp -o server

daemon:
	g++ $(CFLAGS) -I../../code/gen-cpp ../../code/gen-cpp/daemon_types.cpp ../../code/gen-cpp/daemon_constants.cpp ../../code/gen-cpp/WorkDaemon.cpp WorkDaemonClient.cpp -o client

thrift:
	thrift --gen cpp master.thrift

hdfs:
	g++ $(CFLAGS) hdfsConnector.cpp -o hdfs 

clean:
	@rm client
	@rm server
	@rm -r gen-cpp
	@rm hdfs
