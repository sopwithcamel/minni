#include "BucketAggregator.h"

/*
 * Initialize pipeline
 */
BucketAggregator::BucketAggregator(const Config &cfg,
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(char** t, size_t* ts), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 2, num_part, createPAOFunc, destroyPAOFunc),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
	size_t token_size = c_token_size;

	Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
	size_t max_keys_per_token = c_max_keys;

	Setting& c_capacity = readConfigFile(cfg, "minni.aggregator.bucket.capacity");
	capacity = c_capacity;

	Setting& c_nb = readConfigFile(cfg, "minni.aggregator.bucket.num");
	num_buckets = c_nb;

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;

	Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.bucket.aggregate");
	int agg_in_mem = c_agginmem;

	Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
	string inp_type = (const char*)c_inp_typ;

	if (type == Map) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-
		 * token aggregates/writes to buckets. For this pipeline, a 
		 * "token" or a basic pipeline unit is a chunk read from the 
		 * DFS */
		if (!inp_type.compare("chunk")) { 
			chunkreader = new DFSReader(this, map_input, 
					token_size);
			pipeline_list[0].add_filter(*chunkreader);
			toker = new Tokenizer(this, cfg, createPAOFunc,
					max_keys_per_token);
			pipeline_list[0].add_filter(*toker);
		} else if (!inp_type.compare("file")) {
			filereader = new FileReader(this, map_input);
			pipeline_list[0].add_filter(*filereader);
			filetoker = new FileTokenizer(this, cfg, createPAOFunc,
					 max_keys_per_token);
			pipeline_list[0].add_filter(*filetoker);
		}

		creator = new PAOCreator(this, createPAOFunc, max_keys_per_token);
		pipeline_list[0].add_filter(*creator);

	} else if (type == Reduce) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		inp_deserializer = new Deserializer(this, 1, input_file,
			createPAOFunc, destroyPAOFunc);
		pipeline_list[0].add_filter(*inp_deserializer);
		free(input_file);
	}

	if (agg_in_mem) {
		hasher = new Hasher(this, capacity, destroyPAOFunc,
				max_keys_per_token);
		pipeline_list[0].add_filter(*hasher);
		merger = new Merger(this, destroyPAOFunc);
		pipeline_list[0].add_filter(*merger);
	}

	char* bucket_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(bucket_prefix, fprefix.c_str());
	if (type == Map)
		strcat(bucket_prefix, "map-");
	else if (type == Reduce)
		strcat(bucket_prefix, "reduce-");
	strcat(bucket_prefix, "bucket");

	bucket_serializer = new Serializer(this, num_buckets, 
			bucket_prefix, destroyPAOFunc);
	pipeline_list[0].add_filter(*bucket_serializer);
	
	/* Second pipeline: In this pipeline, a token is an entire bucket. In
	 * other words, each pipeline stage is called once for each bucket to
	 * be processed. This may not be fine-grained enough, but should have
	 * enough parallelism to keep our wimpy-node busy. 

	 * In this pipeline, a bucket is read back into memory (converted to 
	 * PAOs again), aggregated using a hashtable, and serialized. */
	deserializer = new Deserializer(this, num_buckets, bucket_prefix,
			createPAOFunc, destroyPAOFunc);
	pipeline_list[1].add_filter(*deserializer);

	bucket_hasher = new Hasher(this, capacity, destroyPAOFunc,
			max_keys_per_token);
	pipeline_list[1].add_filter(*bucket_hasher);
	bucket_merger = new Merger(this, destroyPAOFunc);
	pipeline_list[1].add_filter(*bucket_merger);

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	final_serializer = new Serializer(this, getNumPartitions(), 
			final_path, destroyPAOFunc); 
	pipeline_list[1].add_filter(*final_serializer);

	free(bucket_prefix);
	free(final_path);
}

BucketAggregator::~BucketAggregator()
{
	if (chunkreader)
		delete(chunkreader);
	if (filereader)
		delete(filereader);
	if (toker)
		delete(toker);
	if (inp_deserializer)
		delete(inp_deserializer);
	delete creator;
	if (hasher) {
		delete(hasher);
		delete(merger);
	}
	delete(bucket_serializer);
	delete(deserializer);
	delete(bucket_hasher);
	delete(bucket_merger);
	delete(final_serializer);
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
