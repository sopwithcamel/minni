#ifndef LIB_HASHSORTAGGREGATOR_H
#define LIB_HASHSORTAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "AccumulatorFilter.h"
#include "Adder.h"
#include "CompressTree.h"
#include "CompressTreeFilter.h"
#include "Deserializer.h"
#include "DFSReader.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "Mapper.h"
#include "PAOCreator.h"
#include "PartialAgg.h"
#include "Serializer.h"
#include "Sorter.h"
#include "SparseHashMurmur.h"
#include "SparseHashFilter.h"
#include "TokenizerFilter.h"
#include "util.h"

class HashsortAggregator : public Aggregator {
public:
	HashsortAggregator(const Config &cfg,
                JobID jid,
				AggType type, 
				const uint64_t _partid,
				MapInput* _map_input,
				const char* infile, 
				size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~HashsortAggregator();
private:
	uint64_t capacity; // aggregator capacity

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
	AccumulatorInserter* acc_int_inserter_;
	Serializer* serializer;
	Sorter* sorter;

	Deserializer* deserializer;
	Adder* adder;
	Serializer* final_serializer;

	const char* outfile;
};

#endif // LIB_HASHAGGREGATOR_H
