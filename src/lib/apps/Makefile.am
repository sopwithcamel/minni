EXTRA_DIST = wordcount.cpp wordcount.h# img.cpp img.h pagerank.cpp pagerank.h

pkglib_LTLIBRARIES = wordcount.la# img.la pagerank.la

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

wordcount_la_SOURCES = wordcount.cpp

wordcount_la_LDFLAGS = -module

wordcount_la_CPPFLAGS = $(ALL_CPPFLAGS)
