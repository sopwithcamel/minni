EXTRA_DIST = wordcount.cpp wordcount.h wordcountpao.pb.cc wordcountpao.pb.h wcplain.cpp wcplain.h# img.cpp img.h pagerank.cpp pagerank.h

pkglib_LTLIBRARIES = wordcount.la wcplain.la# img.la pagerank.la

ALL_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../core/ \
			-I$(srcdir)/../data_structures/ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../record_creation/ \
			-I$(srcdir)/../resources/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/ \
			-I/usr/include/CImg/

#img_la_SOURCES = img.cpp

#img_la_LDFLAGS = -module -lX11 -lpthread -ljpeg

#img_la_CPPFLAGS = $(ALL_CPPFLAGS)

#pagerank_la_SOURCES = pagerank.cpp

#pagerank_la_LDFLAGS = -module

#pagerank_la_CPPFLAGS = $(ALL_CPPFLAGS)

wordcount_la_SOURCES = wordcount.cpp wordcountpao.pb.cc
wordcount_la_LDFLAGS = -module -lprotobuf
wordcount_la_CPPFLAGS = $(ALL_CPPFLAGS)

wcplain_la_SOURCES = wcplain.cpp
wcplain_la_LDFLAGS = -module
wcplain_la_CPPFLAGS = $(ALL_CPPFLAGS)
