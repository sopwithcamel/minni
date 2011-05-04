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
#include "InternalHasher.h"
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
	HashAggregator(const uint64_t _capacity, const uint64_t _partid, MapInput* _map_input);
	~HashAggregator();
private:
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer<char*>* toker;
	InternalHasher<char*,PartialAgg*,CharHash, eqstr>* hasher;
};

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(const uint64_t _capacity, const uint64_t _partid, MapInput* _map_input) :
		MapperAggregator(_capacity, _partid),
		map_input(_map_input)
{
	printf("Addr1: %p\n", &pipeline);
	reader = new DFSReader(map_input);
	pipeline.add_filter(*reader);

	toker = new Tokenizer<char*>();
	pipeline.add_filter(*toker);

	hasher = new InternalHasher<char*, PartialAgg*, CharHash, eqstr>();
	pipeline.add_filter(*hasher);
}

HashAggregator::~HashAggregator()
{
	pipeline.clear();
}

#endif // LIB_HASHAGGREGATOR_H
