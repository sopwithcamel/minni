#ifndef LIB_ADDER_H
#define LIB_ADDER_H

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
 * - Consumes: array of sorted PAOs to be merged 
 * - Produces: array of merged PAOs
 */

class Adder : public tbb::filter {
public:
	Adder(Aggregator* agg, PartialAgg* emptyPAO, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~Adder();
	void (*destroyPAO)(PartialAgg* p);
private:
	Aggregator* aggregator;
	PartialAgg* emptyPAO;
	size_t next_buffer;
	PartialAgg*** agged_list;
	FilterInfo** send;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_ADDER_H
