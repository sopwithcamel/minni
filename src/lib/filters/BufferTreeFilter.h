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

static const size_t BUF_SIZE = 65535;

class BufferTreeInserter :
        public AccumulatorInserter
{
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
	~BufferTreeReader();
	void* operator()(void* pao_list);
};
#endif // LIB_BUFFERTREEFILTER_H
