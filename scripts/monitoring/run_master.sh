export TBBROOT="/usr/composer_xe_2011_sp1.9.293/tbb"
tbb_bin="/usr/composer_xe_2011_sp1.9.293/tbb"
if [ -z "$CPATH" ]; then #
    export CPATH="${TBBROOT}/include" #
else #
    export CPATH="${TBBROOT}/include:$CPATH" #
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

cd ~/code/minni/src/master/
./test > /localfs/hamur/master_output
cd -
