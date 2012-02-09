#ifndef LIB_EXTHASHAGGREGATOR_H
#define LIB_EXTHASHAGGREGATOR_H

#include <iostream>
#include <fstream>
#include <string>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Accumulator.h"
#include "AccumulatorFilter.h"
#include "CompressTree.h"
#include "CompressTreeFilter.h"
#include "Mapper.h"
#include "PartialAgg.h"
#include "DFSReader.h"
#include "FileReaderFilter.h"
#include "FileTokenizerFilter.h"
#include "TokenizerFilter.h"
#include "Hashtable.h"
#include "UTHashtable.h"
#include "Hasher.h"
#include "PAOCreator.h"
#include "Serializer.h"
#include "SparseHash.h"
#include "SparseHashFilter.h"
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

    /* data structures */
    Hashtable* hashtable_;
    Accumulator* accumulator_;
    Accumulator* acc_internal_;

	/* for chunk input from DFS */
	MapInput* map_input_; 
	DFSReader* chunkreader_;
	FileReaderFilter* filereader_;
	FileTokenizerFilter* filetoker_;
	TokenizerFilter* toker_;

	/* for serialized PAOs from local file */
	const char* infile_;
	Deserializer* inp_deserializer_;

	PAOCreator* creator_;
	/* internal and external hashing */
	Hasher* hasher_;
	AccumulatorInserter* acc_inserter_;
	AccumulatorReader* acc_reader_;
	AccumulatorInserter* acc_int_inserter_;

	/* scan from external ht into files for reducer */
	const char* outfile_;
};

#endif // LIB_EXTHASHAGGREGATOR_H