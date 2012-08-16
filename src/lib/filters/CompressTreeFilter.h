#ifndef LIB_COMPRESSTREEFILTER_H
#define LIB_COMPRESSTREEFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "AccumulatorFilter.h"
#include "cbt/CompressTree.h"
#include "HashUtil.h"
#include "PartialAgg.h"
#include "Util.h"

class CompressTreeInserter :
        public AccumulatorInserter
{
  public:
	CompressTreeInserter(Aggregator* agg,
            const Config &cfg,
            HashUtil::HashFunction hf,
			const size_t max_keys);
	~CompressTreeInserter();
	void* operator()(void* pao_list);
  private:
    cbt::CompressTree* cbt_;
    HashUtil::HashFunction hf_;
	size_t next_buffer;
	MultiBuffer<FilterInfo>* send_;
    MultiBuffer<PartialAgg*>* evicted_list_;
};

#endif // LIB_COMPRESSTREEFILTER_H
