#ifndef LIB_TOKENIZER_H
#define LIB_TOKENIZER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Mapper.h"
#include "Aggregator.h"
#include "Util.h"

/**
 * To be used after Reader in the pipeline.
 * - Consumes a buffer 
 * - Produces a vector of key, value pairs
 * 
 */

class Tokenizer : public tbb::filter {
public:
	Tokenizer(Aggregator* agg, 
			PartialAgg* (*createPAOFunc)(const char** t),
			const size_t max_keys = DEFAULT_MAX_KEYS_PER_TOKEN);
	~Tokenizer();
private:
	Aggregator* aggregator;
	size_t next_buffer;
	const size_t max_keys_per_token;
	PartialAgg*** pao_list;
	FilterInfo** send;
	PartialAgg* (*createPAO)(const char** token);
	void* operator()(void* pao);
};

#endif // LIB_TOKENIZER_H
