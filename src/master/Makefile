JAVA_HOME=/usr/lib/jvm/default-java
HADOOP_HOME=/home/wolf/Dropbox/CMU/hadoop-0.20.2

INCLUDE_THRIFT=/usr/local/include/thrift
INCLUDE_TBB=/usr/include/tbb
INCLUDE_HADOOP=$(HADOOP_HOME)/src/c++/libhdfs
INCLUDE_JAVA=$(JAVA_HOME)/include
INCLUDE_LINUX_JAVA=$(JAVA_INCLUDE)/linux

LIB_HDFS=$(HADOOP_HOME)/build/c++/Linux-amd64-64/lib
LIB_TBB=/usr/lib/
LIB_JAVA=/usr/lib/jvm/default-java/jre/lib/amd64/server

LINK_THRIFT=thrift
LINK_TBB=tbb
LINK_HDFS=hdfs
LINK_JVM=jvm

INCLUDE=-I$(INCLUDE_THRIFT) -I$(INCLUDE_TBB) -I$(INCLUDE_HADOOP) -I$(INCLUDE_JAVA) -I$(INCLUDE_LINUX_JAVA)
LIB=-L$(LIB_TBB) -L$(LIB_JAVA) -L$(LIB_HDFS)
LINK=-l$(LINK_THRIFT) -l$(LINK_TBB) -l$(LINK_HDFS) -l$(LINK_JVM)

CFLAGS=-Wall $(INCLUDE) $(LIB)  $(LINK)
CXX=g++
THRIFT=thrift

WORKER_DAEMON_STUFF=-I../../code/gen-cpp ../../code/gen-cpp/daemon_types.cpp ../../code/gen-cpp/daemon_constants.cpp ../../code/gen-cpp/WorkDaemon.cpp

all:
	$(CXX) $(CFLAGS) gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Master.cpp gen-cpp/Master_Server.cpp -o server

daemon:
	$(CXX) $(CFLAGS) $(WORKER_DAEMON_STUFF) WorkDaemonClient.cpp -o client

thrift:
	$(THRIFT) --gen cpp master.thrift

hdfs:
	$(CXX) $(CFLAGS) hdfsConnector.cpp -o hdfs 

tbb:
	$(CXX) $(CFLAGS) tbbTest.cpp -o tbb

test:
	@$(CXX) $(CFLAGS) $(WORKER_DAEMON_STUFF) Master.cpp Node.cpp Driver.cpp -o driver \
	&& ./run_hdfs.sh ./driver \
	&& rm driver \
	&& echo "Tests Completed"

clean:
	@rm client
	@rm server
	@rm -r gen-cpp
	@rm hdfs
