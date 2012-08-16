#EXTRA_DIST = wordcount.cpp wordcount.h wordcountpao.pb.cc wordcountpao.pb.h  wcplain.cpp wcplain.h
#			 semantic.cpp semantic. semanticpao.pb.cc semanticpao.pb.h	# img.cpp img.h semantic.cpp semantic.h

pkglib_LTLIBRARIES = wordcount.la
#wcplain.la wchand.la \
#					wcdigram.la semantic.la semantic_plain.la \
#					digplain.la img.la imgclass.la

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
			

#semantic_la_SOURCES = semantic.cpp semanticpao.pb.cc
#semantic_la_LDFLAGS = -module -lprotobuf -lwordnet-3.0
#semantic_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wordcount_la_SOURCES = wordcount.cpp wordcountpao.pb.cc
wordcount_la_LDFLAGS = -module
wordcount_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#wcdigram_la_SOURCES = wcdigram.cpp wcdigram.pb.cc
#wcdigram_la_LDFLAGS = -module -lprotobuf
#wcdigram_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#img_la_SOURCES = img.cpp imgpao.pb.cc
#img_la_LDFLAGS = -module -lprotobuf
#img_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#imgclass_la_SOURCES = imgclass.cpp imgclass.pb.cc
#imgclass_la_LDFLAGS = -module -lprotobuf
#imgclass_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#wcplain_la_SOURCES = wcplain.cpp
#wcplain_la_LDFLAGS = -module
#wcplain_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#wchand_la_SOURCES = wchand.cpp
#wchand_la_LDFLAGS = -module
#wchand_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#semantic_plain_la_SOURCES = semantic_plain.cpp
#semantic_plain_la_LDFLAGS = -module -lwordnet-3.0
#semantic_plain_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
#digplain_la_SOURCES = digplain.cpp
#digplain_la_LDFLAGS = -module
#digplain_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
