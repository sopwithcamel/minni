#EXTRA_DIST = wordcount.cpp wordcount.h wordcountpao.pb.cc wordcountpao.pb.h  wcplain.cpp wcplain.h
#			 semantic.cpp semantic. semanticpao.pb.cc semanticpao.pb.h	# img.cpp img.h semantic.cpp semantic.h

pkglib_LTLIBRARIES = wc_boost.la wc_proto.la \
					wcdig_proto.la wcdig_boost.la \
					wctrig_proto.la wctrig_boost.la \
					pr_proto.la pr_boost.la \
					ic_proto.la ic_boost.la
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
			

pr_proto_la_SOURCES = pr_proto.cpp pagerank.pb.cc
pr_proto_la_LDFLAGS = -module -lprotobuf
pr_proto_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wc_boost_la_SOURCES = wc_boost.cpp
wc_boost_la_LDFLAGS = -module
wc_boost_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
pr_boost_la_SOURCES = pr_boost.cpp
pr_boost_la_LDFLAGS = -module
pr_boost_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wc_proto_la_SOURCES = wc_proto.cpp wcproto.pb.cc
wc_proto_la_LDFLAGS = -module
wc_proto_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wcdig_proto_la_SOURCES = wcdig_proto.cpp wcproto.pb.cc
wcdig_proto_la_LDFLAGS = -module
wcdig_proto_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wcdig_boost_la_SOURCES = wcdig_boost.cpp
wcdig_boost_la_LDFLAGS = -module
wcdig_boost_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wctrig_proto_la_SOURCES = wctrig_proto.cpp wcproto.pb.cc
wctrig_proto_la_LDFLAGS = -module
wctrig_proto_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
wctrig_boost_la_SOURCES = wctrig_boost.cpp
wctrig_boost_la_LDFLAGS = -module
wctrig_boost_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
ic_proto_la_SOURCES = ic_proto.cpp imgcluster.pb.cc
ic_proto_la_LDFLAGS = -module
ic_proto_la_CPPFLAGS = $(ALL_CPPFLAGS)
#
ic_boost_la_SOURCES = ic_boost.cpp
ic_boost_la_LDFLAGS = -module
ic_boost_la_CPPFLAGS = $(ALL_CPPFLAGS)
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
