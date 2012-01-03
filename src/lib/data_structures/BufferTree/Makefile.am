bin_PROGRAMS = bttest

EXTRA_DIST = BufferTree.cpp BufferTree.h Node.cpp Node.h BTTest.cpp

ALL_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../core/ \
			-I$(srcdir)/../data_structures/ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../record_creation/ \
			-I$(srcdir)/../resources/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/\
			-I/home/hamur/code/snappy/

bttest_LDFLAGS = -ltcmalloc

bttest_SOURCES = BufferTree.cpp Node.cpp BTTest.cpp

bttest_CPPFLAGS = $(ALL_CPPFLAGS) -g -Wall -D__STDC_LIMIT_MACROS
