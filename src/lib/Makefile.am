AUTOMAKE_OPTIONS = subdir-objects
SUBDIRS = example apps

lib_LTLIBRARIES = libminni.la 

libminni_la_SOURCES =   Mapper.cpp \
			MapInput.cpp \
		       	Reducer.cpp \
			filters/Adder.cpp \
			filters/Deserializer.cpp \
		       	filters/DFSReader.cpp \
			filters/ExternalHasher.cpp \
			filters/FileReader.cpp \
			filters/FileTokenizer.cpp \
			filters/Hasher.cpp \
			filters/MemCache.cpp \
			filters/Merger.cpp \
			filters/PAOCreator.cpp \
			filters/Serializer.cpp \
			filters/Sorter.cpp \
			filters/Store.cpp \
		       	filters/Tokenizer.cpp \
		       	aggregators/Aggregator.cpp \
			aggregators/BucketAggregator.cpp \
			aggregators/ExthashAggregator.cpp \
			aggregators/HashsortAggregator.cpp \
			aggregators/IterAggregator.cpp \
			aggregators/TesterAggregator.cpp

libminni_la_LIBADD = ../dfs/libdfs.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
		       $(THRIFT_CFLAGS) \
			-I$(srcdir)/../worker/gen-cpp \
			-I$(srcdir)/../master \
			-I$(srcdir)/../dfs \
			-I$(srcdir)/filters/ \
			-I$(srcdir)/aggregators/ \
			-I/home/hamur/leveldb/include

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION) \
			-lltdl -lpthread -ltbb -lconfig++ -lleveldb

minni_includedir = $(includedir)/libminni/libminni

minni_include_HEADERS = PartialAgg.h \
			Mapper.h \
			MapInput.h \
			Reducer.h \
			Minnie.h \
			filters/Adder.h \
			filters/Deserializer.h \
		       	filters/DFSReader.h \
			filters/ExternalHasher.h \
			filters/FileReader.h \
			filters/FileTokenizer.h \
			filters/Hasher.h \
			filters/MemCache.h \
			filters/Merger.h \
			filters/PAOCreator.h \
			filters/Serializer.h \
			filters/Sorter.h \
			filters/Store.h \
			filters/Tokenizer.h \
			aggregators/BucketAggregator.h \
			aggregators/ExthashAggregator.h \
			aggregators/HashsortAggregator.h \
			aggregators/IterAggregator.h \
			aggregators/TesterAggregator.h \
			aggregators/Aggregator.h \
			Defs.h \
			Util.h \
			fnv.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
