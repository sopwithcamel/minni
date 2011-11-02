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

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

/**
 * - Consumes: array of PAOs to be aggregated into an external hashtable
 * - Produces: nil
 */

class ExternalHasher : public tbb::filter {
public:
	ExternalHasher(Aggregator* agg,
			const char* ht_name,
			uint64_t ext_ht_size,
			PartialAgg* emptyPAO, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~ExternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	PartialAgg* emptyPAO;
	Aggregator* aggregator;
	leveldb::DB* db;
	void* operator()(void* pao_list);
};

class ExthashReader : public tbb::filter {
public:
	ExthashReader(Aggregator* agg, 
			const char* ht_name,
			uint64_t ext_capacity,
			PartialAgg* emptyPAO,
			const char* outfile);
	~ExthashReader();
private:
	Aggregator* aggregator;

	/* External hashtable fields */
	leveldb::DB *db;
	const char* ht_name;
	const uint64_t external_capacity;
	PartialAgg* emptyPAO;

	/* Write out fields */
	FILE** fl;
	char* outfile;
	char* buf;

	void* operator()(void*);
};
#endif // LIB_EXTERNALHASHER_H
