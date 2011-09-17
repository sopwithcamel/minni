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
		Aggregator(cfg, type, 2, num_part, createPAOFunc, destroyPAOFunc),
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

	hasher = new Hasher(this, emptyPAO, capacity, destroyPAOFunc);
	pipeline_list[0].add_filter(*hasher);

	char* bucket_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(bucket_prefix, fprefix.c_str());
	if (type == Map)
		strcat(bucket_prefix, "map-");
	else if (type == Reduce)
		strcat(bucket_prefix, "reduce-");
	strcat(bucket_prefix, "bucket");

	bucket_serializer = new Serializer(this, emptyPAO, getNumPartitions(), 
			bucket_prefix, destroyPAOFunc);
	pipeline_list[0].add_filter(*bucket_serializer);

	/* In this pipeline, each bucket is sorted using nsort */
	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	sorter = new Sorter(getNumPartitions(), bucket_prefix, final_path);
	pipeline_list[1].add_filter(*sorter);

	free(final_path);
	free(bucket_prefix);
}

HashsortAggregator::~HashsortAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
