#!/bin/bash
export TBB22_INSTALL_DIR="/opt/intel/tbb/tbb22_013oss" #
tbb_bin="${TBB22_INSTALL_DIR}/build/linux_ia32_gcc_cc4.4.1_libc2.10.1_kernel2.6.31_release" #
if [ -z "$CPATH" ]; then #
    export CPATH="${TBB22_INSTALL_DIR}/include" #
else #
    export CPATH="${TBB22_INSTALL_DIR}/include:$CPATH" #
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