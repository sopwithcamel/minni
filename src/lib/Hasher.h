#ifndef LIB_INTERNALHASHER_H
#define LIB_INTERNALHASHER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Mapper.h"
#include "Aggregator.h"
#include "Util.h"
#include "uthash.h"

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
	Hasher(Aggregator* agg, PartialAgg* emptyPAO, size_t capacity, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~Hasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	Aggregator* aggregator;
	PartialAgg* emptyPAO;
	PartialAgg*** evicted_list;
	PartialAgg*** merge_list;
	PartialAgg*** mergand_list;
	size_t ht_size;
	size_t ht_capacity;
	size_t next_buffer;
	FilterInfo** send;
	PartialAgg* hashtable;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_INTERNALHASHER_H
