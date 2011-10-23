noinst_LTLIBRARIES = libfawnds.la
libfawnds_la_CPPFLAGS = -I$(top_srcdir)/utils -D_FILE_OFFSET_BITS=64 \
			-I$(builddir)/gen-cpp -Icindex -g
libfawnds_la_LIBADD = $(top_builddir)/utils/libfawnkvutils.la


noinst_HEADERS = 	DOMTreeErrorReporter.hpp	\
			DOMWriteErrorHandler.hpp	\
			configuration.h			\
			file_io.h			\
			task.h				\
			value.h				\
			rate_limiter.h			\
            global_limits.h         \
			fawnds_types.h			\
			fawnds_iterator.h		\
			fawnds.h			\
			fawnds_factory.h		\
			hash_functions.h		\
			hash_table_default.h		\
			hash_table_cuckoo.h		\
			file_store.h			\
			sorter.h			\
			fawnds_partition.h		\
			fawnds_combi.h			\
			fawnds_sf.h			\
			fawnds_sf_ordered_trie.h	\
			fawnds_proxy.h			\
			fawnds_monitor.h		\
			bdb.h

libfawnds_la_SOURCES =  DOMTreeErrorReporter.cpp	\
			DOMWriteErrorHandler.cpp	\
			configuration.cc		\
			file_io.cc			\
			task.cc				\
			rate_limiter.cc			\
            global_limits.cc         \
			fawnds_iterator.cc		\
			fawnds.cc			\
			fawnds_factory.cc		\
			hash_functions.cc		\
			hash_table_default.cc		\
			hash_table_cuckoo.cc		\
			file_store.cc			\
			sorter.cc			\
			fawnds_partition.cc		\
			fawnds_combi.cc			\
			fawnds_sf.cc			\
			fawnds_sf_ordered_trie.cc			\
			fawnds_proxy.cc			\
			fawnds_monitor.cc			\
			bdb.cc				\
			cindex/bit_access.cpp			\
			cindex/bit_vector.cpp			\
			cindex/expected_size.cpp			\
			cindex/flat_absoff_bucketing.cpp		\
			cindex/twolevel_absoff_bucketing.cpp	\
			cindex/twolevel_reloff_bucketing.cpp	\
			cindex/semi_direct_16_absoff_bucketing.cpp	\
			cindex/semi_direct_16_reloff_bucketing.cpp	\
			cindex/bucketing_index.cpp			

CLEANFILES = $(BUILT_SOURCES)


# kaminsky: following line forces noinst_* libraries to build
# shared.  This can help with development because a change to
# this library doesn't require re-building libs and programs
# that link against this library
#libfawnds_la_LDFLAGS = -rpath `pwd`

noinst_PROGRAMS = fawnds_bench
fawnds_bench_SOURCES = fawnds_bench.cc
fawnds_bench_CPPFLAGS = -I$(top_srcdir)/utils -I$(builddir)/gen-cpp -g
fawnds_bench_LDADD = $(top_builddir)/utils/libfawnkvutils.la libfawnds.la -lxerces-c

