bin_PROGRAMS = test

EXTRA_DIST = common.h Communicator.h Master.h Node.h Node.h TODO NOTES

test_SOURCES = example/Driver.cpp \
	       Master.cpp \
	       Node.cpp \
	       Communicator.cpp \
		../common/util.cpp

test_CPPFLAGS = $(TBB_CFLAGS) \
		$(THRIFT_CFLAGS) \
		-I$(srcdir)/../lib \
		-I$(srcdir)/../worker/gen-cpp \
		-I$(srcdir)/../common \
		-I$(srcdir)/../dfs

test_LDADD = ../dfs/libdfs.la \
	     ../worker/gen-cpp/libdaemon.la

test_LDFLAGS = $(TBB_LIBS) \
	       $(THRIFT_LIBS) \
	       -lkfsClient \
	       -lkfsIO \
	       -lkfsCommon \
	       -lqcdio \
	       -lboost_regex \
		-lconfig++
