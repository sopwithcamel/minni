lib_LTLIBRARIES = libminni-@MINNI_API_VERSION@.la 

EXTRA_DIST = PartialAgg.h Mapper.h Reducer.h Minnie.h

libminni_@MINNI_API_VERSION@_la_SOURCES =  PartialAgg.cpp \
       					   Mapper.cpp \
					   Reducer.cpp

libminni_@MINNI_API_VERSION@_la_LIBADD = $(srcdir)/../dfs/libdfs.la

libminni_@MINNI_API_VERSION@_la_CPPFLAGS = $(TBB_CFLAGS) \
					   $(THRIFT_CFLAGS) \
					   -I$(srcdir)/../worker/gen-cpp \
					   -I$(srcdir)/../master \
					   -I$(srcdir)/../common \
					   -I$(srcdir)/../dfs

libminni_@MINNI_API_VERSION@_la_LDFLAGS = -version-info $(MINNI_SO_VERSION)

minni_includedir=$(includedir)/libminni-$(MINNI_API_VERSION)/libminni

minni_include_HEADERS = Minnie.h PartialAgg.h Mapper.h Reducer.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni-$(MINNI_API_VERSION).pc
