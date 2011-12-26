#ifndef LIB_SERIALIZER_H
#define LIB_SERIALIZER_H

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
 * Consumes: array of PAOs
 * Produces: nil (PAOs are serialized to one or more files, but this is the 
 *  last stage of the pipeline.
 */

class Serializer : public tbb::filter {
public:
	Serializer(Aggregator* agg,
			const uint64_t nb,
			const char* outfile_prefix, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~Serializer();
private:
	Aggregator* aggregator;
	AggType type;
	void (*destroyPAO)(PartialAgg* p);
	bool already_partitioned;
	int num_buckets;
	FILE** fl;
	void* buf;
	char* outfile_prefix;
	size_t tokens_processed;
	void* operator()(void* pao_list);
	int partition(const char* key);
};

#endif