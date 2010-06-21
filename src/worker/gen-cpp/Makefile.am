bin_PROGRAMS = workdaemon 

EXTRA_DIST = daemon_constants.h daemon_types.h WorkDaemon_common.h WorkDaemon_file.h WorkDaemon.h WorkDaemon_tasks.h

workdaemon_SOURCES = WorkDaemon.cpp \
		     daemon_constants.cpp \
		     daemon_types.cpp \
		     WorkDaemon_server.cpp \
		     WorkDaemon_tasks.cpp \
		     WorkDaemon_file.cpp

workdaemon_CPPFLAGS = $(TBB_CFLAGS) $(THRIFT_CFLAGS) \
		      -I$(srcdir)/../../lib \
		      -I$(srcdir)/../../master \
		      -I$(srcdir)/../../common \
		      -I$(srcdir)/../../dfs

workdaemon_LDADD = $(srcdir)/../../dfs/libdfs.la \
		   $(srcdir)/../../lib/libminni-$(MINNI_API_VERSION).la

workdaemon_LDFLAGS = $(TBB_LIBS) \
		     $(THRIFT_LIBS) \
		     -lm \
		     -lkfsClient \
		     -lkfsIO \
		     -lkfsCommon \
		     -lqcdio \
		     -lboost_regex
