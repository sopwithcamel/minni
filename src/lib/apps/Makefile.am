EXTRA_DIST = knn.cpp knn.h img.cpp img.h

pkglib_LTLIBRARIES = knn.la img.la

knn_la_SOURCES = knn.cpp

knn_la_LDFLAGS = -module

knn_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/

img_la_SOURCES = img.cpp

img_la_LDFLAGS = -module

img_la_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/
