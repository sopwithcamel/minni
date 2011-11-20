#ifndef LIB_HASHSORTAGGREGATOR_H
#define LIB_HASHSORTAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Mapper.h"
#include "PartialAgg.h"
#include "Adder.h"
#include "DFSReader.h"
#include "Hasher.h"
#include "Merger.h"
#include "PAOCreator.h"
#include "Serializer.h"
#include "Sorter.h"
#include "Tokenizer.h"
#include "Deserializer.h"
#include "util.h"

class HashsortAggregator : public Aggregator {
public:
	HashsortAggregator(const Config &cfg,
				AggType type, 
				const uint64_t _partid,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(char** t, size_t* ts), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~HashsortAggregator();
private:
	uint64_t capacity; // aggregator capacity

	/* for chunk input from DFS */
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer* toker;

	/* for serialized PAOs from local file */
	const char* infile;
	Deserializer* inp_deserializer;

	PAOCreator* creator;
	Hasher* hasher;
	Merger* merger;
	Serializer* serializer;
	Sorter* sorter;

	Deserializer* deserializer;
	Adder* adder;
	Serializer* final_serializer;

	const char* outfile;
};

#endif // LIB_HASHAGGREGATOR_H
