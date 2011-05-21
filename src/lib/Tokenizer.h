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

/**
 * To be used after Reader in the pipeline.
 * - Consumes a buffer 
 * - Produces a vector of key, value pairs
 * 
 */

class Tokenizer : public tbb::filter {
public:
	Tokenizer(PartialAgg* (*MapFunc)(const char* t));
	~Tokenizer();
private:
	typedef std::vector< std::pair<char*, PartialAgg*> > KVVector;
	size_t next_buffer;
	KVVector kv_vector[NUM_BUFFERS];
	KVVector* kv;
	PartialAgg* (*Map)(const char* token);
	void* operator()(void* pao);
};

#endif // LIB_TOKENIZER_H
