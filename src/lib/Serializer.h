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
	Serializer(const uint64_t nb, const char* f_prefix);
	~Serializer();
private:
	int num_buckets;
	void* operator()(void* pao_list);
	FILE** fl;
	const char* fname_prefix;
};

#endif
