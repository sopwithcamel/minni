lib_LTLIBRARIES = libminni.la 

EXTRA_DIST = PartialAgg.h Mapper.h Reducer.h Minnie.h

libminni_la_SOURCES =  PartialAgg.cpp \
       					   Mapper.cpp \
					   Reducer.cpp

libminni_la_LIBADD = $(srcdir)/../dfs/libdfs.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
					   $(THRIFT_CFLAGS) \
					   -I$(srcdir)/../worker/gen-cpp \
					   -I$(srcdir)/../master \
					   -I$(srcdir)/../common \
					   -I$(srcdir)/../dfs

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION)

minni_includedir=$(includedir)/libminni/libminni

minni_include_HEADERS = Minnie.h PartialAgg.h Mapper.h Reducer.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
