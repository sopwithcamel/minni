bin_PROGRAMS = workdaemon 
noinst_LTLIBRARIES = libdaemon.la

EXTRA_DIST = daemon_constants.h daemon_types.h WorkDaemon_common.h WorkDaemon_file.h WorkDaemon.h WorkDaemon_tasks.h

workdaemon_SOURCES = WorkDaemon.cpp \
		     daemon_constants.cpp \
		     daemon_types.cpp \
		     WorkDaemon_server.cpp \
		     WorkDaemon_tasks.cpp \
		     WorkDaemon_file.cpp \
			../../common/util.cpp

libdaemon_la_SOURCES = daemon_types.cpp \
		       daemon_constants.cpp \
		       WorkDaemon.cpp

workdaemon_CPPFLAGS = $(TBB_CFLAGS) $(THRIFT_CFLAGS) \
		      -I$(srcdir)/../../lib \
		      -I$(srcdir)/../../lib/core \
		      -I$(srcdir)/../../lib/filters \
		      -I$(srcdir)/../../lib/aggregators \
		      -I$(srcdir)/../../lib/record_creation \
		      -I$(srcdir)/../../lib/data_structures \
		      -I$(srcdir)/../../lib/resources \
		      -I$(srcdir)/../../master \
		      -I$(srcdir)/../../common \
		      -I$(srcdir)/../../dfs

libdaemon_la_CPPFLAGS = $(TBB_CFLAGS) \
			$(THRIFT_CFLAGS)

workdaemon_LDADD = ../../dfs/libdfs.la \
		   ../../lib/libminni.la

workdaemon_LDFLAGS = $(TBB_LIBS) \
		     $(THRIFT_LIBS) \
		     -lm \
		     -lkfsClient \
		     -lkfsIO \
		     -lkfsCommon \
		     -lqcdio \
		     -lboost_regex \
		     -lpthread \
		     -lconfig++ \
			 -lhoard \
			 -ltbb \
			 -lprotobuf \
			 -L/net/hu17/hamur3/libraries/leveldb-1.5.0 -lleveldb
