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
#define	BINSIZE		4096
#define PAOSIZE		32
#define MAX_KEYSIZE	12

class StoreHasher;
class StoreAggregator;
class StoreWriter;

class Store {
	friend class StoreHasher;
	friend class StoreAggregator;
	friend class StoreWriter;
public:
	const size_t max_keys_per_token;

	Store(Aggregator* agg,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys);
	~Store();
	void (*destroyPAO)(PartialAgg* p);
private:
	int store_fd;
	size_t store_size;
	int next_buffer;
	uint64_t tokens_processed;
	Aggregator* aggregator;
	/* Stages of the pipeline */
	StoreHasher* hasher;
	StoreAggregator* agger;
	StoreWriter* writer;

	bool key_present(size_t bin_offset, size_t bin_index);
	void set_key_present(size_t bin_offset, size_t bin_index);
};

/**
 * Consumes: list of PAOs to be externally aggregated
 * Produces: lists of matching offsets and values
 */
class StoreHasher : public tbb::filter {
public:
	StoreHasher(Store* store);
	~StoreHasher();
private:
	Store* store;
	uint64_t tokens_processed;

	/* Pointer to lists holding values to be moved on */
	size_t** offset_list;
	char*** value_list;
	FilterInfo** send;
	size_t next_buffer;

	void* operator()(void* pao_list);
	uint64_t findBinOffset(char* key);
};

/**
 * Consumes: lists of PAOs, offsets and values
 * Produces: lists of merged values and offsets
 */
class StoreAggregator : public tbb::filter {
public:
	StoreAggregator(Store* store);
	~StoreAggregator();
private:
	Store* store;
	uint64_t tokens_processed;

	/* Pointer to lists holding values to be moved on */
	char*** value_list;
	FilterInfo** send;
	size_t next_buffer;

	void* operator()(void* pao_list);
};

/**
 * Consumes: lists of merged values and offsets
 * Produces: -
 */
class StoreWriter : public tbb::filter {
public:
	StoreWriter(Store* store);
	~StoreWriter();
private:
	Store* store;
	uint64_t tokens_processed;
	size_t next_buffer;
	FilterInfo** send;
	void* operator()(void* pao_list);
};

#endif // LIB_STORE_H
