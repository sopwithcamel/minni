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

#define	BINSIZE		4096
#define BINSIZE_BITS	12
#define PAOSIZE		64
#define PAOSIZE_BITS	6
#define SLOTS_PER_BIN	64		// TODO: Get from config
#define MAX_KEYSIZE	8

#define	Hash		HASH_FCN

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

	/* Stages of the pipeline */
	StoreHasher* hasher;
	StoreAggregator* agger;
	StoreWriter* writer;
private:
	// File descriptor for external store
	int store_fd;
	// In-memory array for bin occupancy; set only when pao is stored
	uint64_t* occup;
	// Total number of bins in external store
	size_t store_size;

	int next_buffer;
	uint64_t tokens_processed;
	Aggregator* aggregator;

	/* Checks whether a slot of index = bin_index is occupied in bin
	   given by bin_offset */
	bool slot_occupied(size_t bin_offset, size_t bin_index);
	/* Set the bin_index-th slot in the bin_offset-th bin as occupied */
	void set_slot_occupied(size_t bin_offset, size_t bin_index);
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
	int** flag_list;
	FilterInfo** send;
	size_t next_buffer;

	void* operator()(void* pao_list);
	void findBinOffsetIndex(char* key, uint64_t& bkt);
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
	PartialAgg* rep_hash;
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
