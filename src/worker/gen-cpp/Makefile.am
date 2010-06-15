bin_PROGRAMS = workdaemon 

workdaemon_SOURCES = WorkDaemon.cpp \
		     daemon_constants.cpp \
		     daemon_types.cpp

workdaemon_CPPFLAGS = $(TBB_CFLAGS) $(THRIFT_CFLAGS) -I $(srcdir)/../../lib -I $(srcdir)/../../master

workdaemon_LDFLAGS = $(TBB_LIBS) $(THRIFT_LIBS) -lm
