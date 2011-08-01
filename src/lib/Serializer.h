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
	Serializer(MapperAggregator* agg, const uint64_t nb,
		const char* f_prefix, void (*destroyPAOFunc)(PartialAgg* p));
	~Serializer();
private:
	MapperAggregator* aggregator;
	void (*destroyPAO)(PartialAgg* p);
	int num_buckets;
	FILE** fl;
	const char* fname_prefix;
	void* operator()(void* pao_list);
};

#endif
