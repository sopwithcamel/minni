EXTRA_DIST = wordcountpao.cpp wordcountpao.h

pkglib_LTLIBRARIES = wordcount.la

wordcount_la_SOURCES = wordcountpao.cpp

wordcount_la_LDFLAGS = -module

wordcount_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/

