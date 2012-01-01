bin_PROGRAMS = cttest

EXTRA_DIST = CompressTree.cpp CompressTree.h Node.cpp Node.h CTTest.cpp

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

cttest_LDFLAGS = -L/home/hamur/code/snappy -lsnappy

cttest_SOURCES = CompressTree.cpp Node.cpp CTTest.cpp

cttest_CPPFLAGS = $(ALL_CPPFLAGS) -g -Wall -D__STDC_LIMIT_MACROS
