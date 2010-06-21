#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([Minni], [0.1], [wolf@cs.cmu.edu])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([build-aux/m4])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AC_CONFIG_SRCDIR([src/scripts/plot/plot_memory.m])
AC_CONFIG_HEADERS([src/common/config.h])

# Checks for programs.
AC_PROG_AWK
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_MAKE_SET

# Initialize libtool
LT_INIT

# Needed large file support 
AC_SYS_LARGEFILE

# Checks for libraries.
PKG_CHECK_MODULES(TBB, tbb >= 2.1)
PKG_CHECK_MODULES(THRIFT, thrift >= 0.2.0)
#AC_CHECK_LIB([hdfs], [hdfsConnect], [], [echo "Error.  libhdfs not found." 
#	      				 exit -1])

#AC_CHECK_LIB([jvm], [JNI_CreateJavaVM], [], [echo "Error.  libjvm not found."
#	      				     exit -1])

#AC_CHECK_LIB([m], [cos], [], [echo "Error. lm not found."
#	      		      exit -1])

#AC_CHECK_LIB([tbb], [parallel_for], [], [echo "Error. libtbb not found."
#	      				 exit -1])

#AC_CHECK_LIB([thrift],[_init], [], [echo "Error.  libthrift not found."
#	      			    exit -1])

# Checks for header files.
AC_CHECK_HEADERS([stdint.h stdlib.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([memset socket])

AC_CONFIG_FILES([Makefile
		 src/Makefile
		 src/dfs/Makefile
                 src/lib/Makefile
                 src/master/Makefile
		 src/worker/Makefile
                 src/worker/gen-cpp/Makefile])
AC_OUTPUT
