#ifndef LIB_ACCUMFILTER_H
#define LIB_ACCUMFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Aggregator.h"
#include "PartialAgg.h"
#include "Util.h"

class AccumulatorInserter :
        public tbb::filter 
{
  public:
	AccumulatorInserter(Aggregator* agg,
            const Config &cfg,
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p),
			size_t max_keys);
	virtual ~AccumulatorInserter() {}
	void* operator()(void* pao_list) {}
  protected:
	Aggregator* aggregator_;
    const Config& cfg_;
	const size_t max_keys_per_token;
	uint64_t tokens_processed;
	size_t (*createPAO_)(Token* t, PartialAgg** p);
	void (*destroyPAO)(PartialAgg* p);
};

#endif // LIB_ACCUMFILTER_H
