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
	Deserializer(Aggregator* agg, 
			const uint64_t num_buckets, 
			const char* inp_prefix,
			PartialAgg* emptyPAO,
			PartialAgg* (*createPAOFunc)(const char* k),
			void (*destroyPAOFunc)(PartialAgg* p));
	~Deserializer();
private:
	Aggregator* aggregator;
	PartialAgg* (*createPAO)(const char* k);
	void (*destroyPAO)(PartialAgg* p);
	PartialAgg* emptyPAO;
	char* inputfile_prefix;
	const uint64_t num_buckets;	// Number of files to process serially
	uint64_t buckets_processed;
	PartialAgg*** pao_list;
	size_t next_buffer;

	FILE *cur_bucket;		// file pointer for current bucket
	void* operator()(void*);
	uint64_t appendToList(PartialAgg* p);
};

#endif // LIB_DESERIALIZER_H
