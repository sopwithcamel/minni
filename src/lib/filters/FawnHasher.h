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
#include "Aggregator.h"
#include "Util.h"

#include "fawnds.h"
#include "fawnds_flash.h"
using namespace fawn;

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
	FawnDS<FawnDS_Flash> *evictHash;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

class ExthashReader : public tbb::filter {
public:
	ExthashReader(Aggregator* agg, 
			const char* ht_name,
			uint64_t ext_capacity,
			const char* outfile);
	~ExthashReader();
private:
	Aggregator* aggregator;

	/* External hashtable fields */
	FawnDS<FawnDS_Flash> *ext_hash;
	const char* ht_name;
	const uint64_t external_capacity;
	char* read_buf;

	/* Write out fields */
	FILE** fl;
	char* outfile;

	void* operator()(void*);
};

#endif // LIB_EXTERNALHASHER_H
