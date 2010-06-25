#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([Minni], [0.1],[libminni@googlegroups.com],[minni],[https://sourcery.cmcl.cs.cmu.edu/indefero/p/minni/])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([build-aux/m4])
AM_INIT_AUTOMAKE([1.11 foreign -Wall -Werror])
AM_SILENT_RULES([yes])
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
AC_ARG_WITH([kfs], [AS_HELP_STRING([--with-kfs=DIR], [look for libkfs in DIR])])
FIND_LIBRARY([kfs], [kfsClient], [KFS::ErrorCodeToStr], [kfs/KfsClient.h], [$with_kfs /usr/local /usr])
PKG_CHECK_MODULES(TBB, tbb >= 2.1)
PKG_CHECK_MODULES(THRIFT, thrift >= 0.2.0)

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

# libminni substitutions
AC_SUBST([MINNI_SO_VERSION], [0:0:0])
AC_SUBST([MINNI_API_VERSION], [1.0])

AC_CONFIG_FILES([Makefile
		 src/Makefile
		 src/dfs/Makefile
                 src/lib/Makefile
		 src/lib/libminni.pc:src/lib/libminni.pc.in
		 src/master/Makefile
		 src/worker/Makefile
                 src/worker/gen-cpp/Makefile])
AC_OUTPUT
