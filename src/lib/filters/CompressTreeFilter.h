#ifndef LIB_COMPRESSTREEFILTER_H
#define LIB_COMPRESSTREEFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "Accumulator.h"
#include "AccumulatorFilter.h"
#include "CompressTree.h"
#include "PartialAgg.h"
#include "Util.h"


class CompressTreeInserter :
        public AccumulatorInserter
{
    static const size_t BUF_SIZE = 65535;
    static const uint64_t NUM_BUCKETS = UINT64_MAX;
  public:
	CompressTreeInserter(Aggregator* agg,
			Accumulator* acc,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys);
	~CompressTreeInserter();
	void* operator()(void* pao_list);
  private:
	size_t next_buffer;
	MultiBuffer<FilterInfo>* send_;
    MultiBuffer<PartialAgg*>* evicted_list_;
};

class CompressTreeReader :
        public AccumulatorReader
{
  public:
	CompressTreeReader(Aggregator* agg,
			Accumulator* acc,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			const size_t max_keys);
    CompressTreeReader(Aggregator* agg,
            Accumulator* acc,
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
            const char* outfile_prefix);
	~CompressTreeReader();
	void* operator()(void* pao_list);
};
#endif // LIB_COMPRESSTREEFILTER_H