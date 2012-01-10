SUBDIRS = . tools

noinst_LTLIBRARIES = libdfs.la

EXTRA_DIST = DFS.h KDFS.h

libdfs_la_SOURCES = KDFS.cpp

libdfs_la_CPPFLAGS = $(TBB_CFLAGS) \
		     $(THRIFT_CFLAGS) \
		     -I$(srcdir)/../worker/gen-cpp

libdfs_la_LIBADD =  -lkfsClient \
		    -lkfsIO \
		    -lkfsCommon \
		    -lqcdio \
		    -lboost_regex
