# Makefile for Search
# Erik Zawadzki --- Nov. 10, 2009
# Modified by wolf --- June 5, 2010

#SRC := $(wildcard *.cpp)
#OBJ = $(SRC:.cpp=.o)
.SUFFIXES : .o .cpp

#CXX = gcc

OS=MAC

#TBB
TBB_LIB = -L$(TBB_DIR) -ltbb
ifeq ($(OS),LINUX)
 TBB_DIR = /opt/intel/tbb/tbb22_013oss/build/linux_ia32_gcc_cc4.4.1_libc2.10.1_kernel2.6.31_release
 TBB_INC = -I/opt/intel/tbb/tbb22_013oss/include
else
 TBB_DIR = /Library/Frameworks/Intel_TBB.framework/Versions/tbb22_013oss/build/macos_intel64_gcc_cc4.2.1_os10.6.2_release
 TBB_INC = -I/Library/Frameworks/Intel_TBB.framework/Versions/tbb22_013oss/include
endif

#Thrift 
THR_SRC = WorkDaemon.cpp daemon_constants.cpp daemon_types.cpp
THR_OBJ = $(THR_SRC:.cpp=.o)
THR_INC = -I/usr/local/include/thrift
THR_LIB = -L/usr/local/lib -lthrift 

# Customize
JAVA_HOME=/usr/lib/jvm/default-java
HADOOP_HOME=/home/wolf/Dropbox/CMU/hadoop-0.20.2

INCLUDE_HADOOP=-I$(HADOOP_HOME)/src/c++/libhdfs
INCLUDE_JAVA=-I$(JAVA_HOME)/include
INCLUDE_LINUX_JAVA=-I$(JAVA_INCLUDE)/linux

LIB_HDFS=-L$(HADOOP_HOME)/build/c++/Linux-amd64-64/lib
LIB_JAVA=-L$(JAVA_HOME)/jre/lib/amd64/server

LINK_HDFS=hdfs
LINK_JVM=jvm

# DIR
MASTER=../../master
WRAPPER=../../lib

INCLUDES = $(THR_INC) $(TBB_INC) -I$(MASTER) -I$(WRAPPER)  $(INCLUDE_HADOOP) $(INCLUDE_JAVA) $(INCLUDE_LINUX_JAVA) -I$(WRAPPER) -I. -I$(MASTER)
# -I$(INCLUDE_HADOOP) -I$(INCLUDE_JAVA) -I$(INCLUDE_LINUX_JAVA) -I$(WRAPPER) -I. -I$(MASTER)
LINK= -l$(LINK_HDFS) -l$(LINK_JVM) -lm
LIBFLAGS = $(LIB_HDFS) $(LIB_JAVA) $(LINK) $(THR_LIB) $(TBB_LIB)
# -L$(LIB_JAVA) -L$(LIB_HDFS) $(LINK)


# Generic
LD = ld
CPPFLAGS = -g -O2 


# What is my name!?
SERVER = WorkDaemon_Server
SOBJ = $(THR_OBJ) WorkDaemon_server.o WorkDaemon_tasks.o WorkDaemon_file.o
CLIENT = WorkDaemon_Client
COBJ = $(THR_OBJ) WorkDaemon_client.o WorkDaemon_file.o
TEST = Test
TOBJ = $(THR_OBJ) WorkDaemon_tasks.o WorkDaemon_file.o WorkDaemon_test.o

WOBJ = $(WRAPPER)/Mapper.o $(WRAPPER)/PartialAgg.o $(WRAPPER)/Reducer.o $(MASTER)/HDFS.o

all : $(SERVER) $(CLIENT)

.cpp.o: 
	$(CXX) $(CPPFLAGS) $(INCLUDES) -c $< -o $@

build_wrapper: $(WOBJ)

$(TEST) : $(TOBJ)
	@printf "\n*** building test ***\n"
	$(CXX) $(LIBFLAGS) $(TOBJ) -o $(TEST)

$(SERVER) : $(SOBJ)
	@printf "\n*** building server ***\n"
	$(CXX) $(LIBFLAGS) $(SOBJ) $(WOBJ) -o $(SERVER)

$(CLIENT) : $(COBJ)
	@printf "\n*** building test client ***\n"
	$(CXX) $(LIBFLAGS)  $(COBJ) -o $(CLIENT)  

clean:
	rm *.o $(SERVER) $(CLIENT)

remake:
	make clean; make
