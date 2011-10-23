#hashdb_test code
noinst_PROGRAMS = testFawnDS testIterator testTrie testCuckoo testCombi testByYCSBWorkload benchCuckoo benchSemiRandomWrite benchStores preprocessTrace
testFawnDS_SOURCES = testFawnDS.cc
testFawnDS_CPPFLAGS = 				\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

testFawnDS_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

testIterator_SOURCES = testIterator.cc
testIterator_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

testIterator_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

testTrie_SOURCES = testTrie.cc
testTrie_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

testTrie_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

testCuckoo_SOURCES = testCuckoo.cc
testCuckoo_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

testCuckoo_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

testCombi_SOURCES = testCombi.cc
testCombi_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

testCombi_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)


testByYCSBWorkload_SOURCES= testByYCSBWorkload.cc
testByYCSBWorkload_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

testByYCSBWorkload_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

benchCuckoo_SOURCES = benchCuckoo.cc
benchCuckoo_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

benchCuckoo_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

benchSemiRandomWrite_SOURCES = benchSemiRandomWrite.cc
benchSemiRandomWrite_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

benchSemiRandomWrite_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)

benchStores_SOURCES = benchStores.cc
benchStores_CPPFLAGS = 			\
	-I$(top_srcdir)/utils 			\
	-I$(top_srcdir)/fawnds			\
	-I$(top_builddir)/fawnds		\
	-I$(top_builddir)/fawnds/gen-cpp

benchStores_LDADD = 				\
	$(top_builddir)/fawnds/libfawnds.la 	\
	$(top_builddir)/utils/libfawnkvutils.la \
	$(THRIFT_LIBS)


preprocessTrace_SOURCES= preprocessTrace.cc

preprocessTrace_CPPFLAGS =

preprocessTrace_LDADD =

