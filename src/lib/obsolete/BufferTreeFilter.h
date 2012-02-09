#ifndef LIB_BUFFERTREEFILTER_H
#define LIB_BUFFERTREEFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "Accumulator.h"
#include "AccumulatorFilter.h"
#include "BufferTree.h"
#include "PartialAgg.h"
#include "Util.h"


class BufferTreeInserter :
        public AccumulatorInserter
{
    static const size_t BUF_SIZE = 65535;
    static const uint64_t NUM_BUCKETS = UINT64_MAX;
  public:
	BufferTreeInserter(Aggregator* agg,
			Accumulator* acc,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys);
	~BufferTreeInserter();
	void* operator()(void* pao_list);
};

class BufferTreeReader :
        public AccumulatorReader
{
  public:
	BufferTreeReader(Aggregator* agg,
			Accumulator* acc,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			const size_t max_keys);
    BufferTreeReader(Aggregator* agg,
            Accumulator* acc,
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
            const char* outfile_prefix);
	~BufferTreeReader();
	void* operator()(void* pao_list);
};
#endif // LIB_BUFFERTREEFILTER_H
