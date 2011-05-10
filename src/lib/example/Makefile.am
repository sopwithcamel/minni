EXTRA_DIST = gen_pao.h gen_pao.cpp gen_mapper.h gen_mapper.cpp wordcountmapper.h wordcountmapper.cpp wordcountpao.cpp wordcountpao.h

pkglib_LTLIBRARIES = wordcount.la gen.la

wordcount_la_SOURCES = wordcountmapper.cpp wordcountpao.cpp

wordcount_la_LDFLAGS = -module 

wordcount_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/

gen_la_SOURCES = gen_pao.cpp gen_mapper.cpp

gen_la_LDFLAGS = -module 

gen_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/
