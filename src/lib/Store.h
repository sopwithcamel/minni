#ifndef LIB_STORE_H
#define LIB_STORE_H

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
#include "uthash.h"

#define	BINS		100000000
#define	SETSIZE		8		

class Store {
public:
	const size_t max_keys_per_token;

	Store(Aggregator* agg);
	~Store();
	void (*destroyPAO)(PartialAgg* p);
private:
	

	Aggregator* aggregator;
	/* Stages of the pipeline */
	StoreHasher* hasher;
	StoreAggregator* agger;
	StoreWriter* writer;
};

/**
 * Consumes: list of PAOs to be externally aggregated
 * Produces: lists of matching offsets and values
 */
class StoreHasher : public Store, public tbb::filter {
public:
	StoreHasher();
	~StoreHasher();
private:
	uint64_t tokens_processed;

	/* Pointer to lists holding values to be moved on */
	size_t** offset_list;
	char*** value_list;
	FilterInfo** send;
	size_t next_buffer;

	void* operator()(void* pao_list);
}

/**
 * Consumes: lists of PAOs, offsets and values
 * Produces: lists of merged values and offsets
 */
class StoreAggregator : public Store, public tbb::filter {
public:
	StoreAggregator();
	~StoreAggregator();
private:
	uint64_t tokens_processed;

	/* Pointer to lists holding values to be moved on */
	char*** value_list;
	FilterInfo** send;
	size_t next_buffer;

	void* operator()(void* pao_list);
}

/**
 * Consumes: lists of merged values and offsets
 * Produces: -
 */
class StoreWriter : public Store, public tbb::filter {
public:
	StoreWriter();
	~StoreWriter();
private:
	uint64_t tokens_processed;
	size_t next_buffer;
	FilterInfo** send;
	void* operator()(void* pao_list);
}

#endif // LIB_STORE_H
