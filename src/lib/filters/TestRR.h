#ifndef LIB_EXTERNALHASHER_H
#define LIB_EXTERNALHASHER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Mapper.h"
#include "Aggregator.h"
#include "Util.h"

#define NUM_READS	2000000

/**
 * - Consumes: array of PAOs to be aggregated into an external hashtable
 * - Produces: nil
 */

class ExternalHasher : public tbb::filter {
public:
	ExternalHasher(Aggregator* agg,
			const char* ht_name,
			uint64_t ext_ht_size,
			void (*destroyPAOFunc)(PartialAgg* p));
	~ExternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	Aggregator* aggregator;
	int fd;
	long arr[NUM_READS];
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_EXTERNALHASHER_H
