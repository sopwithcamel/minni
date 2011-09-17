#ifndef LIB_PLAINMAPPER_H
#define LIB_PLAINMAPPER_H

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
#include "Deserializer.h"
#include "util.h"

class PlainMapper : public Aggregator {
public:
	PlainMapper(const Config &cfg, 
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile);
	~PlainMapper();
private:
	/* for chunk input from DFS */
	MapInput* map_input; 
	DFSReader* reader;
	Tokenizer* toker;
	Serializer* final_serializer;
	const char* outfile;
};

#endif // LIB_PLAINMAPPER_H
