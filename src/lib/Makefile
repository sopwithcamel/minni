HDFS=-I/home/abalacha/Map-Reduce-athula-code/hadoop-0.20.2/src/c++/libhdfs -L/home/abalacha/Map-Reduce-athula-code/hadoop-0.20.2/build/c++/Linux-i386-32/lib -I/usr/lib/jvm/java-6-sun/include -I/usr/lib/jvm/java-6-sun/include/linux -L/usr/lib/jvm/java-6-sun/jre/lib/i386/server -lhdfs -ljvm

JAVA_HOME=/usr/lib/jvm/default-java
HADOOP_HOME=/home/wolf/Dropbox/CMU/hadoop-0.20.2

INCLUDE_TBB=/usr/include/tbb
INCLUDE_HADOOP=$(HADOOP_HOME)/src/c++/libhdfs
INCLUDE_JAVA=$(JAVA_HOME)/include
INCLUDE_LINUX_JAVA=$(JAVA_INCLUDE)/linux

LIB_HDFS=$(HADOOP_HOME)/build/c++/Linux-amd64-64/lib
LIB_JAVA=/usr/lib/jvm/default-java/jre/lib/amd64/server
LIB_TBB=/usr/lib/

LINK_HDFS=hdfs
LINK_JVM=jvm
LINK_TBB=tbb

#HDFS=-I$(INCLUDE_HADOOP) -I$(INCLUDE_JAVA) -I$(INCLUDE_LINUX_JAVA) -L$(LIB_HDFS) -L$(LIB_JAVA) -l$(LINK_HDFS) -l$(LINK_JVM)

all: PartialAgg.o Mapper.o Reducer.o 

PartialAgg.o: PartialAgg.cpp PartialAgg.h
	g++ -c PartialAgg.cpp


Minnie.o: Minnie.cpp Minnie.h PartialAgg.h
	g++ -c Minnie.cpp

Mapper.o: Mapper.cpp Mapper.h  
	g++ -c Mapper.cpp -I /usr/local/include/thrift/ -I../worker/gen-cpp/ -I../master/ -I tbb/ $(HDFS)

Reducer.o: Reducer.cpp Reducer.h
	g++ -c Reducer.cpp -I /usr/local/include/thrift/ -I../worker/gen-cpp/ -I../master/ -I tbb/ $(HDFS)

wordcount: example/wordcount.cpp 
	g++ example/wordcount.cpp -I. -I/usr/local/include/thrift/ -I../worker/gen-cpp -I../master/ -I tbb/ -I$(INCLUDE_TBB) -L$(LIB_TBB) -l$(LINK_TBB) -shared $(HDFS) -o example/wordcount.so


#Minnie.o: Minnie.cpp Minnie.h tbb/task.h tbb/tbb_thread.h 
#	g++ -c Minnie.cpp -I tbb30_20100406oss/include/tbb/ 

clean:
	rm *.o
