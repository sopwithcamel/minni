#ifndef LIB_HASHAGGREGATOR_H
#define LIB_HASHAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>
#include <libconfig.h++>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Mapper.h"
#include "PartialAgg.h"
#include "DFSReader.h"
#include "Tokenizer.h"
#include "Hasher.h"
#include "Serializer.h"
#include "Deserializer.h"
#include "hashutil.h"

typedef struct 
{
	size_t operator()(const char* str) const
	{ 
		return MurmurHash(str, strlen(str), 42);
	}
} CharHash;

struct eqstr
{
	bool operator()(const char* s1, const char* s2) const
	{
//		printf("Comparing %s and %s; ", s1, s2);
		return strcmp(s1, s2) == 0;	
	}
};

class HashAggregator : public Aggregator {
public:
	HashAggregator(Config* cfg, 
				const uint64_t type,
				const uint64_t _partid, 
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~HashAggregator();
private:
	const uint64_t type;	// where to get the input from
	uint64_t capacity; // aggregator capacity

	/* for chunk input from DFS */
	MapInput* map_input;
	DFSReader* reader;
	Tokenizer* toker;

	/* for serialized PAOs from local file */
	const char* infile;
	Deserializer* deserializer;

	Hasher<char*, CharHash, eqstr>* hasher;

	/* to serialized PAOs to local file */
	Serializer* serializer;

	uint64_t num_buckets;
	const char* outfile;
};

#endif // LIB_HASHAGGREGATOR_H
