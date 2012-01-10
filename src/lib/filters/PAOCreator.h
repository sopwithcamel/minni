#ifndef LIB_PAOCREATOR_H
#define LIB_PAOCREATOR_H

#include <stdlib.h>
#include <assert.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "Tokenizer.h"
#include "Util.h"

/**
 * - Consumes a list of tokens 
 * - Produces a list of PAOs
 */

class PAOCreator : public tbb::filter {
public:
	PAOCreator(Aggregator* agg, 
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			size_t max_keys);
	~PAOCreator();
private:
	Aggregator* aggregator;
	size_t next_buffer;
	const size_t max_keys_per_token;
	MultiBuffer<PartialAgg*>* pao_list;
	MultiBuffer<FilterInfo>* send;
	size_t (*createPAO)(Token* t, PartialAgg** p);
	void* operator()(void* pao);
};

#endif // LIB_PAOCREATOR_H
