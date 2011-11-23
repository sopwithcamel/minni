#ifndef LIB_TOKENIZERFILTER_H
#define LIB_TOKENIZERFILTER_H

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
#include "DelimitedTokenizer.h"
#include "Util.h"

/**
 * To be used after Reader in the pipeline.
 * - Consumes a buffer 
 * - Produces a vector of key, value pairs
 * 
 */

class TokenizerFilter : public tbb::filter {
public:
	TokenizerFilter(Aggregator* agg, const Config& cfg,
			PartialAgg* (*createPAOFunc)(Token* t),
			const size_t max_keys = DEFAULT_MAX_KEYS_PER_TOKEN);
	~TokenizerFilter();
private:
	Aggregator* aggregator;
	MemCache* memCache;
	size_t next_buffer;
	const size_t max_keys_per_token;
	DelimitedTokenizer* chunk_tokenizer;
	MultiBuffer<FilterInfo>* send;
	PartialAgg* (*createPAO)(Token* token);
	void* operator()(void* pao);
};

#endif // LIB_TOKENIZERFILTER_H
