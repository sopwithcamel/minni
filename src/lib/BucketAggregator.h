#ifndef LIB_BUCKETAGGREGATOR_H
#define LIB_BUCKETAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Hash.h"
#include "Mapper.h"
#include "PartialAgg.h"
#include "DFSReader.h"
#include "Tokenizer.h"
#include "Hasher.h"
#include "Serializer.h"
#include "Deserializer.h"

class BucketAggregator : public Aggregator {
public:
	BucketAggregator(Config* cfg, 
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~BucketAggregator();
private:
	uint64_t capacity; // aggregator capacity

	/* for chunk input from DFS */
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer* toker;

	/* for serialized PAOs from local file */
	const char* infile;
	Deserializer* inp_deserializer;

	Hashtable* hasher;
	Serializer* bucket_serializer;
	Deserializer* deserializer;
	Hashtable* bucket_hasher;
	Serializer* final_serializer;
	uint64_t num_buckets;
	const char* outfile;
};

#endif // LIB_HASHAGGREGATOR_H
