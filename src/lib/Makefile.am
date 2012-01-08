AUTOMAKE_OPTIONS = subdir-objects
SUBDIRS = apps data_structures/CompressTree

lib_LTLIBRARIES = libminni.la 

libminni_la_SOURCES =   core/Mapper.cpp \
			core/MapInput.cpp \
			core/Reducer.cpp \
			filters/Adder.cpp \
			filters/Deserializer.cpp \
			filters/DFSReader.cpp \
			filters/ExternalHasher.cpp \
			filters/FileReaderFilter.cpp \
			filters/FileTokenizerFilter.cpp \
			filters/Hasher.cpp \
			filters/MemCache.cpp \
			filters/Merger.cpp \
			filters/PAOCreator.cpp \
			filters/Serializer.cpp \
			filters/Sorter.cpp \
			filters/Store.cpp \
			filters/TokenizerFilter.cpp \
			aggregators/Aggregator.cpp \
			aggregators/BucketAggregator.cpp \
			aggregators/ExthashAggregator.cpp \
			aggregators/HashsortAggregator.cpp \
			aggregators/TesterAggregator.cpp \
			record_creation/Tokenizer.cpp \
			record_creation/FileTokenizer.cpp \
			record_creation/DelimitedTokenizer.cpp \
			data_structures/UTHashtable.cpp \
			data_structures/BufferTree.cpp \
			data_structures/BTNode.cpp

libminni_la_LIBADD = ../dfs/libdfs.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
					   $(THRIFT_CFLAGS) \
						-D__STDC_LIMIT_MACROS
					   -I$(srcdir)/../worker/gen-cpp \
					   -I$(srcdir)/../master \
					   -I$(srcdir)/../dfs \
					   -I$(srcdir)/core/ \
					   -I$(srcdir)/filters/ \
					   -I$(srcdir)/aggregators/ \
					   -I$(srcdir)/record_creation/ \
					   -I$(srcdir)/data_structures/ \
					   -I$(srcdir)/resources/ \
					   -I/home/hamur/leveldb/include

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION) \
			-lltdl -lpthread -ltbb -lconfig++ -lleveldb

minni_includedir = $(includedir)/libminni/libminni

minni_include_HEADERS = core/PartialAgg.h \
			core/Mapper.h \
			core/MapInput.h \
			core/Reducer.h \
			core/Minnie.h \
			core/Defs.h \
			core/Util.h \
			filters/Adder.h \
			filters/Deserializer.h \
			filters/DFSReader.h \
			filters/ExternalHasher.h \
			filters/FileReaderFilter.h \
			filters/FileTokenizerFilter.h \
			filters/Hasher.h \
			filters/MemCache.h \
			filters/Merger.h \
			filters/PAOCreator.h \
			filters/Serializer.h \
			filters/Sorter.h \
			filters/Store.h \
			filters/TokenizerFilter.h \
			aggregators/BucketAggregator.h \
			aggregators/ExthashAggregator.h \
			aggregators/HashsortAggregator.h \
			aggregators/TesterAggregator.h \
			aggregators/Aggregator.h \
			record_creation/FileTokenizer.h \
			record_creation/DelimitedTokenizer.h \
			record_creation/Tokenizer.h \
			data_structures/Accumulator.h \
			data_structures/Hashtable.h \
			data_structures/UTHashtable.h \
			data_structures/BufferTree.h \
			data_structures/BTNode.h \
			resources/ElasticObject.h

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
