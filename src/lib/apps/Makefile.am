EXTRA_DIST = img.cpp img.h pagerank.cpp pagerank.h wordcountpao.cpp wordcountpao.h

pkglib_LTLIBRARIES = img.la pagerank.la wordcount.la

#knn_la_SOURCES = knn.cpp

#knn_la_LDFLAGS = -module

#knn_la_CPPFLAGS = $(THRIFT_CFLAGS) \
#			-I$(srcdir)/../ \
#			-I$(srcdir)/../filters/ \
#			-I$(srcdir)/../aggregators/ \
#			-I$(srcdir)/../../worker/gen-cpp \
#			-I$(srcdir)/../../dfs/

img_la_SOURCES = img.cpp

img_la_LDFLAGS = -module -lX11 -lpthread -ljpeg

img_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../core/ \
			-I$(srcdir)/../data_structures/ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../record_creation/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/ \
			-I/usr/include/CImg/

pagerank_la_SOURCES = pagerank.cpp

pagerank_la_LDFLAGS = -module

pagerank_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../core/ \
			-I$(srcdir)/../data_structures/ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../record_creation/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/


wordcount_la_SOURCES = wordcountpao.cpp

wordcount_la_LDFLAGS = -module

wordcount_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../core/ \
			-I$(srcdir)/../data_structures/ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../record_creation/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/

