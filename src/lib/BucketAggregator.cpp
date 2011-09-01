#include "config.h"
#include "BucketAggregator.h"

/*
 * Initialize pipeline
 */
BucketAggregator::BucketAggregator(Config* cfg,
				const uint64_t type, 
				const uint64_t _partid,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, 2, _partid, createPAOFunc, destroyPAOFunc),
		type(DFS_CHUNK_INPUT),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	string fprefix;
	string empty_key;
	try {
		Setting& c_empty_key = cfg->lookup("minni.common.key.empty");
		empty_key = (const char*)c_empty_key;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}		
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	try {
		Setting& c_capacity = cfg->lookup("minni.aggregator.bucket.capacity");
		capacity = c_capacity;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}		

	try {
		Setting& c_nb = cfg->lookup("minni.aggregator.bucket.num");
		num_buckets = c_nb;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}		


	try {
		Setting& c_fprefix = cfg->lookup("minni.common.file_prefix");
		fprefix = (const char*)c_fprefix;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}		


	if (DFS_CHUNK_INPUT == type) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (LOCAL_PAO_INPUT == type) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		inp_deserializer = new Deserializer(this, 1/*TODO: how many?*/, input_file,
			emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*inp_deserializer);
		free(input_file);
	}

	hasher = new Hashtable(this, emptyPAO, capacity, destroyPAOFunc);
	if (LOCAL_PAO_INPUT == type)
		hasher->setFlushOnComplete();
	pipeline_list[0].add_filter(*hasher);

	char* bucket_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(bucket_prefix, fprefix.c_str());
	strcat(bucket_prefix, "bucket");

	bucket_serializer = new Serializer(this, emptyPAO, num_buckets, 
			bucket_prefix, destroyPAOFunc);
	pipeline_list[0].add_filter(*bucket_serializer);
	
	/* Second pipeline: In this pipeline, a token is an entire bucket. In
	 * other words, each pipeline stage is called once for each bucket to
	 * be processed. This may not be fine-grained enough, but should have
	 * enough parallelism to keep our wimpy-node busy. 

	 * In this pipeline, a bucket is read back into memory (converted to 
	 * PAOs again), aggregated using a hashtable, and serialized. */
	deserializer = new Deserializer(this, num_buckets, bucket_prefix,
			emptyPAO, createPAOFunc);
	pipeline_list[1].add_filter(*deserializer);

	bucket_hasher = new Hashtable(this, emptyPAO, capacity, destroyPAOFunc);
	bucket_hasher->setFlushOnComplete();
	pipeline_list[1].add_filter(*bucket_hasher);

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	final_serializer = new Serializer(this, emptyPAO, (uint64_t)1, final_path, 
			destroyPAOFunc); 
	pipeline_list[1].add_filter(*final_serializer);

	free(bucket_prefix);
	free(final_path);
}

BucketAggregator::~BucketAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
