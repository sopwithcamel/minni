#ifndef LIB_DESERIALIZER_H
#define LIB_DESERIALIZER_H

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
#include "Util.h"

/**
 * To be used as the input stage in the pipeline. It deserializes a list of 
 * PAOs from a list of files. The location prefix is passed as an input
 * parameter and "bucketN" is appended to the prefix to get the absolute
 * path of the file.
 * - Produces an array of PAOs; the size of the array is determined by an
 *   an input parameter.  
 */

class Deserializer : public tbb::filter {
public:
	static const size_t n_buffer = NUM_BUFFERS;
	Deserializer(MapperAggregator* agg, const uint64_t num_buckets, 
		const char* inp_prefix, PartialAgg* emptyPAO);
	~Deserializer();
private:
	MapperAggregator* aggregator;
	PartialAgg* emptyPAO;
	const char* inputfile_prefix;
	const uint64_t num_buckets;	// Number of files to process serially
	uint64_t buckets_processed;
	PartialAgg** pao_list;
	void* operator()(void*);
};

#endif // LIB_DESERIALIZER_H
