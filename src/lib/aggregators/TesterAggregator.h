#ifndef LIB_TESTERAGGREGATOR_H
#define LIB_TESTERAGGREGATOR_H

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
#include "Sorter.h"
#include "Serializer.h"
#include "Deserializer.h"
#include "Store.h"
#include "util.h"

class TesterAggregator : public Aggregator {
public:
	TesterAggregator(const Config &cfg, 
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p),
				const char* outfile);
	~TesterAggregator();
private:
	string test_element;

	/* For testing tokenizer */
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer* toker;
	
	/* For testing sorter */
	Sorter* sorter;
	char* sort_in_file;
	char* sort_out_file;

	/* For testing external store */
	Store* store;
};

#endif // LIB_TESTERAGGREGATOR_H
