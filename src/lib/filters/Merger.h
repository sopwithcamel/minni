#ifndef LIB_MERGER_H
#define LIB_MERGER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Mapper.h"
#include "Aggregator.h"
#include "Util.h"

/**
 * - Consumes: array of PAOs to be merged and destroyed
 * - Produces: Nothing. Simply passes on the list of PAOs to be serialized
 * 
 */

class Merger : public tbb::filter {
public:
	Merger(Aggregator* agg, void (*destroyPAOFunc)(PartialAgg* p));
	~Merger();
	void (*destroyPAO)(PartialAgg* p);
private:
	Aggregator* aggregator;
	size_t next_buffer;
	FilterInfo** send;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_MERGER_H
