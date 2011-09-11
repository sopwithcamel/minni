#include "HashAggregator.h"

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(const Config &cfg,
				AggType type, // where to read from
				const uint64_t num_part, 
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 1, num_part, createPAOFunc, destroyPAOFunc),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	Setting& c_empty_key = readConfigFile(cfg, "minni.common.key.empty");
	string empty_key = (const char*)c_empty_key;
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	Setting& c_capacity = readConfigFile(cfg, "minni.aggregator.hashtable_internal.capacity");
	capacity = c_capacity;

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;

	if (type == Map) {
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (type == Reduce) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		deserializer = new Deserializer(this, 1, input_file,
			emptyPAO, createPAOFunc, destroyPAOFunc);
		pipeline_list[0].add_filter(*deserializer);
		free(input_file);
	}

	hasher = new Hashtable(this, emptyPAO, capacity, destroyPAOFunc);
	pipeline_list[0].add_filter(*hasher);

	// TODO: Handle output to DFS here
	char* output_file = (char*)malloc(FILENAME_LENGTH);
	strcpy(output_file, fprefix.c_str());
	strcat(output_file, outfile);
	serializer = new Serializer(this, emptyPAO, getNumPartitions(), output_file, 
		destroyPAOFunc);
	pipeline_list[0].add_filter(*serializer);
	free(output_file);
}

HashAggregator::~HashAggregator()
{
	pipeline_list[0].clear();
}
