SUBDIRS = example

lib_LTLIBRARIES = libminni.la 

EXTRA_DIST = 		PartialAgg.h \
			Mapper.h \
			Reducer.h \
			Minnie.h \
			Deserializer.h \
		       	DFSReader.h \
			ExternalHasher.h \
			FileReader.h \
			Hasher.h \
			Serializer.h \
			Sorter.h \
			Tokenizer.h \
			BucketAggregator.h \
			ExthashAggregator.h \
			HashAggregator.h \
			HashsortAggregator.h \
			Aggregator.h \
			Defs.h \
			Util.h \
			fnv.h

libminni_la_SOURCES =  	PartialAgg.cpp \
		      	Mapper.cpp \
		       	Reducer.cpp \
			Deserializer.cpp \
		       	DFSReader.cpp \
			ExternalHasher.cpp \
		       	FileReader.cpp \
			Serializer.cpp \
			Hasher.cpp \
			Sorter.cpp \
		       	Tokenizer.cpp \
			BucketAggregator.cpp \
			ExthashAggregator.cpp \
		       	HashAggregator.cpp \
			HashsortAggregator.cpp \
		       	Aggregator.cpp \
			Util.cpp

libminni_la_LIBADD = ../dfs/libdfs.la \
			/home/hamur/fawnkv/fawnds/libfawnds.la \
			/home/hamur/fawnkv/utils/libfawnkvutils.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
		       $(THRIFT_CFLAGS) \
		       -I$(srcdir)/../worker/gen-cpp \
		       -I$(srcdir)/../master \
		       -I$(srcdir)/../dfs \
			-I/home/hamur/fawnkv/fawnds \
			-I/home/hamur/fawnkv/utils \
			-I/home/hamur/code/leveldb/include

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION) \
			-lltdl -lpthread -ltbb -lconfig++ -lleveldb

minni_includedir = $(includedir)/libminni/libminni

minni_include_HEADERS = PartialAgg.h \
			Mapper.h \
			Reducer.h \
			Minnie.h \
			Deserializer.h \
		       	DFSReader.h \
			ExternalHasher.h \
			FileReader.h \
			Hasher.h \
			Serializer.h \
			Sorter.h \
			Tokenizer.h \
			BucketAggregator.h \
			ExthashAggregator.h \
			HashAggregator.h \
			HashsortAggregator.h \
			Aggregator.h \
			Defs.h \
			Util.h \
			fnv.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
