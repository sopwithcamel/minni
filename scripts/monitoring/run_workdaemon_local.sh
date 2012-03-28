export TBBROOT=/usr/composer_xe_2011_sp1.9.293/tbb

TBB_ARCH="/cc4.1.0_libc2.4_kernel2.6.16.21"

if [ -z "${LD_LIBRARY_PATH}" ]
then
   LD_LIBRARY_PATH="$TBBROOT/lib/intel64/$TBB_ARCH"; export LD_LIBRARY_PATH
else
   LD_LIBRARY_PATH="$TBBROOT/lib/intel64/$TBB_ARCH:${LD_LIBRARY_PATH}"; export LD_LIBRARY_PATH
fi
time ~/code/minni/src/worker/gen-cpp/workdaemon > /localfs/hamur/wlog
