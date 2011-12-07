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
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "TokenizerFilter.h"
#include "Hasher.h"
#include "Merger.h"
#include "PAOCreator.h"
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
				size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~ExthashAggregator();
private:
	uint64_t internal_capacity; // aggregator capacity
	uint64_t external_capacity; // external hashtable capacity

	leveldb::DB* hash_table;

	/* for chunk input from DFS */
	MapInput* map_input; 
	DFSReader* chunkreader;
	FileReaderFilter* filereader;
	FileTokenizerFilter* filetoker;
	TokenizerFilter* toker;

	/* for serialized PAOs from local file */
	const char* infile;
	Deserializer* inp_deserializer;

	PAOCreator* creator;
	/* internal and external hashing */
	Hasher* hasher;
	Merger* merger;
	ExternalHasher* ext_hasher;

	/* scan from external ht into files for reducer */
	Serializer* final_serializer; 
	const char* outfile;
};

#endif // LIB_EXTHASHAGGREGATOR_H
