AUTOMAKE_OPTIONS = subdir-objects
SUBDIRS = apps

lib_LTLIBRARIES = libminni.la 

libminni_la_SOURCES =   core/Mapper.cpp \
			core/MapInput.cpp \
			core/Reducer.cpp \
			filters/AccumulatorFilter.cpp \
			filters/Adder.cpp \
			filters/CompressTreeFilter.cpp \
			filters/ConcurrentHashFilter.cpp \
			filters/Deserializer.cpp \
			filters/DFSReader.cpp \
			filters/ExternalHasher.cpp \
			filters/FileChunkerFilter.cpp \
			filters/FileReaderFilter.cpp \
			filters/FileTokenizerFilter.cpp \
			filters/LocalReader.cpp \
			filters/MemCache.cpp \
			filters/PAOCreator.cpp \
			filters/Serializer.cpp \
			filters/Sorter.cpp \
			filters/SparseHashFilter.cpp \
			filters/TokenizerFilter.cpp \
			aggregators/Aggregator.cpp \
			aggregators/BucketAggregator.cpp \
			aggregators/ExthashAggregator.cpp \
			aggregators/HashsortAggregator.cpp \
			record_creation/Tokenizer.cpp \
			record_creation/FileTokenizer.cpp \
			record_creation/DelimitedTokenizer.cpp \
			data_structures/SparseHashMurmur.cpp \
			data_structures/SparseHashBob.cpp \
			utils/HashUtil.cpp

libminni_la_LIBADD = ../dfs/libdfs.la

libminni_la_CPPFLAGS = $(TBB_CFLAGS) \
					   $(THRIFT_CFLAGS) \
						-D__STDC_LIMIT_MACROS \
					   -I$(srcdir)/../worker/gen-cpp \
					   -I$(srcdir)/../master \
					   -I$(srcdir)/../dfs \
					   -I$(srcdir)/core/ \
					   -I$(srcdir)/filters/ \
					   -I$(srcdir)/aggregators/ \
					   -I$(srcdir)/record_creation/ \
					   -I$(srcdir)/data_structures/ \
					   -I$(srcdir)/resources/ \
					   -I$(srcdir)/apps/ \
					   -I$(srcdir)/utils \
					   -I/usr/local/lib/leveldb-1.5.0/include/

libminni_la_LDFLAGS = -version-info $(MINNI_SO_VERSION) \
			-L/usr/lib/tbb \
			-lltdl -lpthread -ltbb -lconfig++ -lsnappy -lssl -lhoard \
			-lprotobuf -lboost_serialization \
			-L/usr/local/lib/leveldb-1.5.0 -lleveldb -lcbt

minni_includedir = $(includedir)/libminni/libminni

pkgconfigdir = $(libdir)/pkgconfig

pkgconfig_DATA = libminni.pc
