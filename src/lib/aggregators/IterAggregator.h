#ifndef LIB_ITERAGGREGATOR_H
#define LIB_ITERAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Mapper.h"
#include "PartialAgg.h"
#include "DFSReader.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "TokenizerFilter.h"
#include "Hasher.h"
#include "Merger.h"
#include "ExternalHasher.h"
#include "Serializer.h"
#include "Deserializer.h"
#include "PAOCreator.h"
#include "Util.h"
#include "util.h"

class IterAggregator : public Aggregator {
public:
	IterAggregator(const Config &cfg,
				AggType type,
				const uint64_t _partid, 
				MapInput* _map_input,
				const char* infile, 
				size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~IterAggregator();
	
	uint64_t curr_iter; // current iteration
private:
	uint64_t internal_capacity; // aggregator capacity
	uint64_t external_capacity; // external hashtable capacity

	leveldb::DB** curr_hash;
	leveldb::DB** prev_hash;

	/* for chunk input from DFS */
	MapInput* map_input; 
	DFSReader* chunkreader;
	FileReaderFilter* filereader;
	FileTokenizerFilter* filetoker;
	TokenizerFilter* toker;

	ExternalHashReader* hash_reader;
	PAOCreator* creator;

	/* for serialized PAOs from local file */
	const char* infile;
	Deserializer* inp_deserializer;

	/* current iteration data management */
	ExternalHasher* ext_hasher;
	ExternalHashSerializer* ehash_serializer;
	const char* outfile;

	/* check for convergence */
	bool repeatPipeline(uint64_t it);
};

#endif // LIB_ITERAGGREGATOR_H
