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
	Adder(Aggregator* agg, size_t max_keys);
	~Adder();
private:
	Aggregator* aggregator;
	const size_t max_keys_per_token;
	size_t next_buffer;
	MultiBuffer<PartialAgg*>* agged_list;
	MultiBuffer<FilterInfo>* send;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_ADDER_H
