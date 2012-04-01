#ifndef LIB_SPARSEHASHFILTER_H
#define LIB_SPARSEHASHFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "Accumulator.h"
#include "AccumulatorFilter.h"
#include "SparseHash.h"
#include "PartialAgg.h"
#include "Util.h"


class SparseHashInserter :
        public AccumulatorInserter
{
    static const size_t BUF_SIZE = 65535;
    static const uint64_t NUM_BUCKETS = UINT64_MAX;
  public:
	SparseHashInserter(Aggregator* agg,
			Accumulator* acc,
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p),
            int num_part,
			const size_t max_keys);
	~SparseHashInserter();
    int partition(const std::string& key);
	void* operator()(void* pao_list);
  private:
    int numPartitions_;
	size_t next_buffer;
	MultiBuffer<FilterInfo>* send_;
    MultiBuffer<PartialAgg*>* evicted_list_;
	size_t (*createPAO_)(Token* t, PartialAgg** p);
};

/*
class SparseHashReader :
        public AccumulatorReader
{
  public:
	SparseHashReader(Aggregator* agg,
			Accumulator* acc,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			const size_t max_keys);
    SparseHashReader(Aggregator* agg,
            Accumulator* acc,
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
            const char* outfile_prefix);
	~SparseHashReader();
	void* operator()(void* pao_list);
};
*/
#endif // LIB_SPARSEHASHFILTER_H
