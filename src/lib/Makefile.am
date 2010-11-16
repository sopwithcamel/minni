lib_LTLIBRARIES = libminni.la 

EXTRA_DIST = PartialAgg.h FawnMapper.h Reducer.h Minnie.h EHFawn.h

libminni_la_SOURCES =  PartialAgg.cpp \
		       FawnMapper.cpp \
		       Reducer.cpp \
			EHFawn.cpp

libminni_la_LIBADD = ../dfs/libdfs.la \
			/home/hamur/fawnkv/fawnds/libfawnds.la \
			/home/hamur/fawnkv/utils/libfawnkvutils.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
		       $(THRIFT_CFLAGS) \
		       -I$(srcdir)/../worker/gen-cpp \
		       -I$(srcdir)/../master \
		       -I$(srcdir)/../common \
		       -I$(srcdir)/../dfs \
			-I/home/hamur/fawnkv/fawnds \
			-I/home/hamur/fawnkv/utils \
			-lpthread

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION)

minni_includedir = $(includedir)/libminni/libminni

minni_include_HEADERS = Minnie.h PartialAgg.h FawnMapper.h EHFawn.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
