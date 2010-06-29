bin_PROGRAMS = cpToDFS cpFromDFS

EXTRA_DIST = Reader.h Writer.h

cpToDFS_SOURCES = Writer.cpp

cpFromDFS_SOURCES = Reader.cpp

cpToDFS_LDADD = ../libdfs.la

cpFromDFS_LDADD = ../libdfs.la

cpToDFS_CPPFLAGS = $(TBB_CFLAGS) \
		   $(THRIFT_CFLAGS) \
		   -I$(srcdir)/../ \
		   -I$(srcdir)/../../worker/gen-cpp \
		   -I$(srcdir)/../../config

cpFromDFS_CPPFLAGS = $(TBB_CFLAGS) \
		     $(THRIFT_CFLAGS) \
		     -I$(srcdir)/../ \
		     -I$(srcdir)/../../worker/gen-cpp \
		     -I$(srcdir)/../../config

cpToDFS_LDFLAGS = -lkfsClient \
		  -lkfsIO \
		  -lkfsCommon \
		  -lqcdio \
		  -lboost_regex

cpFromDFS_LDFLAGS = -lkfsClient \
		  -lkfsIO \
		  -lkfsCommon \
		  -lqcdio \
		  -lboost_regex
