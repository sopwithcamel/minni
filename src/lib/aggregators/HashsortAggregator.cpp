#include "HashsortAggregator.h"

/*
 * Initialize pipeline
 */
HashsortAggregator::HashsortAggregator(const Config &cfg,
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char** t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 3, num_part, createPAOFunc, destroyPAOFunc),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
	size_t token_size = c_token_size;

	Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
	size_t max_keys_per_token = c_max_keys;

	Setting& c_capacity = readConfigFile(cfg, "minni.aggregator.hashsort.capacity");
	capacity = c_capacity;

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;

	Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.hashsort.aggregate");
	int agg_in_mem = c_agginmem;

	Setting& c_nsort_mem = readConfigFile(cfg, "minni.aggregator.hashsort.nsort_mem");
	int nsort_mem = c_nsort_mem;

	if (type == Map) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
		reader = new DFSReader(this, map_input, token_size);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, cfg, createPAOFunc, max_keys_per_token);
		pipeline_list[0].add_filter(*toker);
	} else if (type == Reduce) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		inp_deserializer = new Deserializer(this, 1/*TODO: how many?*/, input_file,
			createPAOFunc, destroyPAOFunc);
		pipeline_list[0].add_filter(*inp_deserializer);
		free(input_file);
	}

	if (agg_in_mem) {
		hasher = new Hasher(this, capacity, destroyPAOFunc, max_keys_per_token);
		pipeline_list[0].add_filter(*hasher);
		merger = new Merger(this, destroyPAOFunc);
		pipeline_list[0].add_filter(*merger);
	}

	char* sort_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(sort_prefix, fprefix.c_str());
	if (type == Map)
		strcat(sort_prefix, "map-");
	else if (type == Reduce)
		strcat(sort_prefix, "reduce-");

	char* unsorted_file = (char*)malloc(FILENAME_LENGTH);
	strcpy(unsorted_file, sort_prefix);
	strcat(unsorted_file, "unsorted");

	serializer = new Serializer(this, 1/*create one unsorted file*/, 
			unsorted_file, destroyPAOFunc);
	pipeline_list[0].add_filter(*serializer);

	/* In the second pipeline, each bucket is sorted using nsort */
	char* sorted_file = (char*)malloc(FILENAME_LENGTH);
	strcpy(sorted_file, sort_prefix);
	strcat(sorted_file, "sorted");

	sorter = new Sorter(1, unsorted_file, sorted_file, nsort_mem);
	pipeline_list[1].add_filter(*sorter);
	
	/* In this pipeline, the sorted file is deserialized into
	 * PAOs again, aggregated and serialized. */
	deserializer = new Deserializer(this, 1, sorted_file,
			createPAOFunc, destroyPAOFunc);
	pipeline_list[2].add_filter(*deserializer);

	adder = new Adder(this, destroyPAOFunc);
	pipeline_list[2].add_filter(*adder);

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	final_serializer = new Serializer(this, getNumPartitions(), 
			final_path, destroyPAOFunc); 
	pipeline_list[2].add_filter(*final_serializer);

	free(final_path);
	free(sort_prefix);
	free(unsorted_file);
	free(sorted_file);
}

HashsortAggregator::~HashsortAggregator()
{
	if (reader)
		delete(reader);
	if (toker)
		delete(toker);
	if (inp_deserializer)
		delete(inp_deserializer);
	if (hasher) {
		delete(hasher);
		delete(merger);
	}
	delete(serializer);
	delete(sorter);
	delete(deserializer);
	delete(adder);
	delete(final_serializer);
	pipeline_list[0].clear();
	pipeline_list[1].clear();
	pipeline_list[2].clear();
}
