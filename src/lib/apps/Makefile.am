EXTRA_DIST = img.cpp img.h #knn.cpp knn.h

pkglib_LTLIBRARIES = img.la #knn.la 

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
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/ \
			-I/usr/include/CImg/
