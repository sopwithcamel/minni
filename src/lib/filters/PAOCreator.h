#ifndef LIB_PAOCREATOR_H
#define LIB_PAOCREATOR_H

#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <libconfig.h++>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "MemCache.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "Util.h"

/**
 * - Consumes a list of tokens 
 * - Produces a list of PAOs
 */

class PAOCreator : public tbb::filter {
public:
	PAOCreator(Aggregator* agg, 
			PartialAgg* (*createPAOFunc)(char** t, size_t* ts),
			const size_t max_keys = DEFAULT_MAX_KEYS_PER_TOKEN);
	~PAOCreator();
private:
	Aggregator* aggregator;
	MemCache* memCache;
	size_t next_buffer;
	const size_t max_keys_per_token;
	MultiBuffer<PartialAgg*>* pao_list;
	MultiBuffer<FilterInfo>* send;
	PartialAgg* (*createPAO)(char** token, size_t* ts);
	void* operator()(void* pao);
};

#endif // LIB_PAOCREATOR_H
