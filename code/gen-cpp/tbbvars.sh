#!/bin/bash
tbb_root="/opt/intel/tbb/tbb22_013oss" #
tbb_bin="/opt/intel/tbb/tbb22_013oss/build/linux_ia32_gcc_cc4.4.1_libc2.10.1_kernel2.6.31_release" #
if [ -z "$CPATH" ]; then #
    export CPATH="${tbb_root}/include" #
else #
    export CPATH="${tbb_root}/include:$CPATH" #
fi #
if [ -z "$LIBRARY_PATH" ]; then #
    export LIBRARY_PATH="${tbb_bin}" #
else #
    export LIBRARY_PATH="${tbb_bin}:$LIBRARY_PATH" #
fi #
if [ -z "$LD_LIBRARY_PATH" ]; then #
    export LD_LIBRARY_PATH="${tbb_bin}" #
else #
    export LD_LIBRARY_PATH="${tbb_bin}:$LD_LIBRARY_PATH" #
fi #
 #
