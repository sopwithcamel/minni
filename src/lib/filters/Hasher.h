#ifndef LIB_INTERNALHASHER_H
#define LIB_INTERNALHASHER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Hashtable.h"
#include "PartialAgg.h"
#include "Mapper.h"
#include "Aggregator.h"
#include "Util.h"

/**
 * - Consumes: array of PAOs to be aggregated
 * - Produces: array of PAOs evicted from in-memory HT 
 * 
 * TODO:
 * - overload += operator in user-defined PartialAgg class
 * - pass in partitioning function as a parameter
 * - change to TBB hash_map which is parallel access
 */

class Hasher : public tbb::filter {
public:
	Hasher(Aggregator* agg, Hashtable* ht, 
			void (*destroyPAOFunc)(PartialAgg* p),
			size_t max_keys);
	~Hasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	Aggregator* aggregator;
	MultiBuffer<PartialAgg*>* evicted_list;
	MultiBuffer<PartialAgg*>* merge_list;
	MultiBuffer<PartialAgg*>* mergand_list;
	const size_t max_keys_per_token;
	size_t next_buffer;
    Hashtable* hashtable;
	MultiBuffer<FilterInfo>* send;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_INTERNALHASHER_H
