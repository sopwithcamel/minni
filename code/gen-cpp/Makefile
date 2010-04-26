# Makefile for Search
# Erik Zawadzki --- Nov. 10, 2009

#SRC := $(wildcard *.cpp)
#OBJ = $(SRC:.cpp=.o)
.SUFFIXES : .o .cpp

OS = LINUX

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

WOLF = /Users/epz/local/os_repo/src/master/gen-cpp

# Generic
LD = ld
LIBFLAGS  = $(THR_LIB) $(TBB_LIB) -L$(WOLF)
CPPFLAGS = -g -O2 
INCLUDES = $(THR_INC) $(TBB_INC) -I$(WOLF)

# What is my name!?
SERVER = WorkDaemon_Server
SOBJ = $(THR_OBJ) WorkDaemon_server.o WorkDaemon_tasks.o WorkDaemon_other.o
CLIENT = WorkDaemon_Client
COBJ = $(THR_OBJ) WorkDaemon_client2.o $(WOLF)/Master.o

all : $(SERVER) $(CLIENT)


.cpp.o: 
	$(CXX) $(CPPFLAGS) $(INCLUDES) -c $< -o $@

$(SERVER) : $(SOBJ)
	@printf "*** building server ***"
	$(CXX) $(LIBFLAGS) $(SOBJ) -o $(SERVER)

$(CLIENT) : $(COBJ)
	@printf "*** building test client ***"
	$(CXX) $(LIBFLAGS)  $(COBJ) -o $(CLIENT)  

clean:
	rm *.o $(SERVER) $(CLIENT)

remake:
	make clean; make
