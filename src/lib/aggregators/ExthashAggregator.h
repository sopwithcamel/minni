#ifndef LIB_EXTHASHAGGREGATOR_H
#define LIB_EXTHASHAGGREGATOR_H

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
#include "Merger.h"
#include "ExternalHasher.h"
#include "Serializer.h"
#include "Deserializer.h"
#include "Util.h"
#include "util.h"

class ExthashAggregator : public Aggregator {
public:
	ExthashAggregator(const Config &cfg,
				AggType type,
				const uint64_t _partid, 
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char** t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~ExthashAggregator();
private:
	uint64_t internal_capacity; // aggregator capacity
	uint64_t external_capacity; // external hashtable capacity

	/* for chunk input from DFS */
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer* toker;

	/* for serialized PAOs from local file */
	const char* infile;
	Deserializer* inp_deserializer;

	/* internal and external hashing */
	Hasher* hasher;
	Merger* merger;
	ExternalHasher* ext_hasher;

	/* scan from external ht into files for reducer */
	Serializer* final_serializer; 
	const char* outfile;
};

#endif // LIB_EXTHASHAGGREGATOR_H
