bin_PROGRAMS = workdaemon 
noinst_LTLIBRARIES = libdaemon.la

EXTRA_DIST = daemon_constants.h daemon_types.h WorkDaemon_common.h WorkDaemon_file.h WorkDaemon.h WorkDaemon_tasks.h

workdaemon_SOURCES = WorkDaemon.cpp \
		     daemon_constants.cpp \
		     daemon_types.cpp \
		     WorkDaemon_server.cpp \
		     WorkDaemon_tasks.cpp \
		     WorkDaemon_file.cpp

libdaemon_la_SOURCES = daemon_types.cpp \
		       daemon_constants.cpp \
		       WorkDaemon.cpp

workdaemon_CPPFLAGS = $(TBB_CFLAGS) $(THRIFT_CFLAGS) \
		      -I$(srcdir)/../../lib \
		      -I$(srcdir)/../../master \
		      -I$(srcdir)/../../common \
		      -I$(srcdir)/../../dfs

libdaemon_la_CPPFLAGS = $(TBB_CFLAGS) \
			$(THRIFT_CFLAGS)

workdaemon_LDADD = ../../dfs/libdfs.la \
		   ../../lib/libminni.la \
			/home/hamur/fawnkv/fawnds/libfawnds.la \
			/home/hamur/fawnkv/utils/libfawnkvutils.la

workdaemon_LDFLAGS = $(TBB_LIBS) \
		     $(THRIFT_LIBS) \
		     -lm \
		     -lkfsClient \
		     -lkfsIO \
		     -lkfsCommon \
		     -lqcdio \
		     -lboost_regex
