#ifndef LIB_EXTERNALHASHER_H
#define LIB_EXTERNALHASHER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Mapper.h"
#include "MapperAggregator.h"
#include "Util.h"

#include "fawnds.h"
#include "fawnds_flash.h"
using namespace fawn;

#define HT_CAPACITY		500000

/**
 * - Consumes: array of PAOs to be aggregated into an external hashtable
 * - Produces: nil
 */

class ExternalHasher : public tbb::filter {
public:
	ExternalHasher(MapperAggregator* agg,
			char* ht_name,
			PartialAgg* emptyPAO, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~ExternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	PartialAgg* emptyPAO;
	MapperAggregator* aggregator;
	FawnDS<FawnDS_Flash> *evictHash;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

#endif // LIB_EXTERNALHASHER_H
