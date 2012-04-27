EXTRA_DIST = wordcount.cpp wordcount.h wordcountpao.pb.cc wordcountpao.pb.h  wcplain.cpp wcplain.h
#			 semantic.cpp semantic. semanticpao.pb.cc semanticpao.pb.h	# img.cpp img.h semantic.cpp semantic.h

pkglib_LTLIBRARIES = wordcount.la wcplain.la img.la semantic.la#semantic.la

ALL_CPPFLAGS = $(THRIFT_CFLAGS) \
			-I$(srcdir)/../ \
			-I$(srcdir)/../aggregators/ \
			-I$(srcdir)/../core/ \
			-I$(srcdir)/../data_structures/ \
			-I$(srcdir)/../filters/ \
			-I$(srcdir)/../record_creation/ \
			-I$(srcdir)/../resources/ \
			-I$(srcdir)/../../worker/gen-cpp \
			-I$(srcdir)/../../dfs/ \
			-I/usr/include/CImg/ \
			-I/usr/local/WordNet-3.0/include/
			

img_la_SOURCES = img.cpp imgpao.pb.cc
img_la_LDFLAGS = -module -lX11 -lpthread -ljpeg -lprotobuf
img_la_CPPFLAGS = $(ALL_CPPFLAGS)

semantic_la_SOURCES = semantic.cpp semanticpao.pb.cc
semantic_la_LDFLAGS = -module -lprotobuf -lwordnet-3.0
semantic_la_CPPFLAGS = $(ALL_CPPFLAGS)

wordcount_la_SOURCES = wordcount.cpp wordcountpao.pb.cc
wordcount_la_LDFLAGS = -module -lprotobuf
wordcount_la_CPPFLAGS = $(ALL_CPPFLAGS)

wcplain_la_SOURCES = wcplain.cpp
wcplain_la_LDFLAGS = -module
wcplain_la_CPPFLAGS = $(ALL_CPPFLAGS)
