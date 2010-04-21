#!/bin/bash
tbb_root="/Library/Frameworks/Intel_TBB.framework/Versions/tbb22_013oss" #
tbb_bin="/Library/Frameworks/Intel_TBB.framework/Versions/tbb22_013oss/build/macos_intel64_gcc_cc4.2.1_os10.6.2_release" #
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
if [ -z "$DYLD_LIBRARY_PATH" ]; then #
    export DYLD_LIBRARY_PATH="${tbb_bin}" #
else #
    export DYLD_LIBRARY_PATH="${tbb_bin}:$DYLD_LIBRARY_PATH" #
fi #
 #
