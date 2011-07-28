SUBDIRS = example

lib_LTLIBRARIES = libminni.la 

EXTRA_DIST = 		PartialAgg.h \
			Mapper.h \
			Reducer.h \
			Minnie.h \
			FileReader.h \
			Tokenizer.h \
			Hasher.h \
			Serializer.h \
			HashAggregator.h \
			MapperAggregator.h \
			Defs.h \
			Util.h \
			fnv.h

libminni_la_SOURCES =  	PartialAgg.cpp \
		      	Mapper.cpp \
		       	Reducer.cpp \
		       	hashutil.cpp \
		       	MapperAggregator.cpp \
		       	HashAggregator.cpp \
		       	FileReader.cpp \
		       	DFSReader.cpp \
		       	Tokenizer.cpp \
			Serializer.cpp \
			Util.cpp

libminni_la_LIBADD = ../dfs/libdfs.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
		       $(THRIFT_CFLAGS) \
		       -I$(srcdir)/../worker/gen-cpp \
		       -I$(srcdir)/../master \
		       -I$(srcdir)/../common \
		       -I$(srcdir)/../dfs \
			-lpthread -ltbb

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION) \
			-lltdl

minni_includedir = $(includedir)/libminni/libminni

minni_include_HEADERS = Minnie.h \
			PartialAgg.h \
			Mapper.h \
			FileReader.h \
			Tokenizer.h \
			Hasher.h \
			HashAggregator.h \
			MapperAggregator.h \
			Defs.h \
			Util.h \
			fnv.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
