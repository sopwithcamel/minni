bin_PROGRAMS = test

test_SOURCES = example/Driver.cpp \
	       Master.cpp \
	       Node.cpp \
	       Communicator.cpp \
	       HDFS.cpp \
	       $(srcdir)/../worker/gen-cpp/daemon_types.cpp \
	       $(srcdir)/../daemon_constants.cpp \
	       $(srcdir)/../WorkDaemon.cpp

test_CPPFLAGS = -I$(srcdir)/../lib \
		-I$(srcdir)/../worker/gen-cpp
