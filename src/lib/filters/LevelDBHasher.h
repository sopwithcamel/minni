#ifndef LIB_EXTERNALHASHER_H
#define LIB_EXTERNALHASHER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Aggregator.h"
#include "Util.h"

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/cache.h"

/**
 * - Consumes: array of PAOs to be aggregated into an external hashtable
 * - Produces: nil
 */

class ExternalHasher : public tbb::filter {
public:
	ExternalHasher(Aggregator* agg,
			leveldb::DB** dbp,
			const char* ht_name,
			uint64_t ext_ht_size,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys);
	~ExternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	Aggregator* aggregator;
	const size_t max_keys_per_token;
	leveldb::DB** db;
	char* buf;
	void* operator()(void* pao_list);
};

class ExternalHashReader : public tbb::filter {
public:
	ExternalHashReader(Aggregator* agg,
			leveldb::DB** dbp,
			const char* ht_name,
			uint64_t ext_ht_size,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			const size_t max_keys);
	~ExternalHashReader();
	size_t (*createPAO)(Token* t, PartialAgg** p);
private:
	Aggregator* aggregator;
	size_t next_buffer;
	const size_t max_keys_per_token;
	leveldb::DB** db;
	MultiBuffer<FilterInfo>* send;
	MultiBuffer<PartialAgg*>* pao_list;
	void* operator()(void* pao_list);
};

class ExternalHashSerializer : public tbb::filter {
public:
	ExternalHashSerializer(Aggregator* agg, 
			leveldb::DB** dbp,
			const char* ht_name,
			uint64_t ext_capacity,
			const char* outfile);
	~ExternalHashSerializer();
private:
	Aggregator* aggregator;

	/* External hashtable fields */
	leveldb::DB** db;
	const char* ht_name;
	const uint64_t external_capacity;

	/* Write out fields */
	FILE** fl;
	char* outfile;
	char* buf;

	void* operator()(void*);
};
#endif // LIB_EXTERNALHASHER_H
