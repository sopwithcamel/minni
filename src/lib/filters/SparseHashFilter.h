#ifndef LIB_SPARSEHASHFILTER_H
#define LIB_SPARSEHASHFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "AccumulatorFilter.h"
#include "SparseHashMurmur.h"
#include "SparseHashBob.h"
#include "PartialAgg.h"
#include "Util.h"


class SparseHashInserter :
        public AccumulatorInserter
{
    static const size_t BUF_SIZE = 65535;
    static const uint64_t NUM_BUCKETS = UINT64_MAX;
  public:
	SparseHashInserter(Aggregator* agg,
            const Config &cfg,
            int num_part,
			const size_t max_keys);
	~SparseHashInserter();
    int partition(const char* key) const;
	void* operator()(void* pao_list);
  private:
    SparseHashMurmur* sparsehash_;
    int numPartitions_;
	size_t next_buffer;
	MultiBuffer<FilterInfo>* send_;
    MultiBuffer<PartialAgg*>* evicted_list_;
};
#endif // LIB_SPARSEHASHFILTER_H
