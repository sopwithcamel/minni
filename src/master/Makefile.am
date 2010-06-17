bin_PROGRAMS = test

EXTRA_DIST = common.h Communicator.h DFS.h KDFS.h Master.h Node.h Node.h TODO NOTES

test_SOURCES = example/Driver.cpp \
	       Master.cpp \
	       Node.cpp \
	       Communicator.cpp \
	       $(srcdir)/../worker/gen-cpp/daemon_types.cpp \
	       $(srcdir)/../worker/gen-cpp/daemon_constants.cpp \
	       $(srcdir)/../worker/gen-cpp/WorkDaemon.cpp

test_CPPFLAGS = $(TBB_CFLAGS) \
		$(THRIFT_CFLAGS) \
		-I$(srcdir)/../lib \
		-I$(srcdir)/../worker/gen-cpp \
		-I$(srcdir)/../common \
		-I$(srcdir)/../dfs

test_LDADD = $(srcdir)/../dfs/libdfs.la

test_LDFLAGS = $(TBB_LIBS) \
	       $(THRIFT_LIBS) \
	       -lkfsClient \
	       -lkfsIO \
	       -lkfsCommon \
	       -lqcdio \
	       -lboost_regex
