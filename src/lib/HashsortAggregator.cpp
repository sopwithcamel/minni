#include "HashsortAggregator.h"

/*
 * Initialize pipeline
 */
HashsortAggregator::HashsortAggregator(const Config &cfg,
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 3, num_part, createPAOFunc, destroyPAOFunc),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	Setting& c_empty_key = readConfigFile(cfg, "minni.common.key.empty");
	string empty_key = (const char*)c_empty_key;
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	Setting& c_capacity = readConfigFile(cfg, "minni.aggregator.hashsort.capacity");
	capacity = c_capacity;

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;

	Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.hashsort.aggregate");
	int agg_in_mem = c_agginmem;

	if (type == Map) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (type == Reduce) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		inp_deserializer = new Deserializer(this, 1/*TODO: how many?*/, input_file,
			emptyPAO, createPAOFunc, destroyPAOFunc);
		pipeline_list[0].add_filter(*inp_deserializer);
		free(input_file);
	}

	if (agg_in_mem) {
		hasher = new Hasher(this, emptyPAO, capacity, destroyPAOFunc);
		pipeline_list[0].add_filter(*hasher);
		merger = new Merger(this, emptyPAO, destroyPAOFunc);
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

	serializer = new Serializer(this, emptyPAO, 1/*create one unsorted file*/, 
			unsorted_file, destroyPAOFunc);
	pipeline_list[0].add_filter(*serializer);

	/* In the second pipeline, each bucket is sorted using nsort */
	char* sorted_file = (char*)malloc(FILENAME_LENGTH);
	strcpy(sorted_file, sort_prefix);
	strcat(sorted_file, "sorted");

	sorter = new Sorter(1, unsorted_file, sorted_file);
	pipeline_list[1].add_filter(*sorter);
	
	/* In this pipeline, the sorted file is deserialized into
	 * PAOs again, aggregated and serialized. */
	deserializer = new Deserializer(this, num_buckets, sorted_file,
			emptyPAO, createPAOFunc, destroyPAOFunc);
	pipeline_list[2].add_filter(*deserializer);

	adder = new Adder(this, emptyPAO, destroyPAOFunc);
	pipeline_list[2].add_filter(*adder);

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	final_serializer = new Serializer(this, emptyPAO, getNumPartitions(), 
			final_path, destroyPAOFunc); 
	// we've already partitioned these keys once. Use that!
	final_serializer->setInputAlreadyPartitioned();
	pipeline_list[1].add_filter(*final_serializer);

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
