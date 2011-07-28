#ifndef LIB_HASHAGGREGATOR_H
#define LIB_HASHAGGREGATOR_H

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
#include "Tokenizer.h"
#include "Hasher.h"
#include "Serializer.h"
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

class HashAggregator : public MapperAggregator {
public:
	HashAggregator(const uint64_t _capacity, const uint64_t _partid, 
			MapInput* _map_input, PartialAgg* (*MapFunc)(const char* t), 
			void (*destroyPAOFunc)(PartialAgg* p), 
			const uint64_t num_buckets, const char* outfile_prefix);
	~HashAggregator();
private:
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer* toker;
	Hasher<char*, CharHash, eqstr>* hasher;
	Serializer* serializer;
	const uint64_t num_buckets;
	const char* outfile_prefix;
};

#endif // LIB_HASHAGGREGATOR_H
