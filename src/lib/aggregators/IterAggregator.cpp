#include "IterAggregator.h"

/*
 * Initialize pipeline
 */
IterAggregator::IterAggregator(const Config& cfg,
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
	Setting& c_int_capacity = readConfigFile(cfg, 
			"minni.aggregator.hashtable_external.capacity.internal");
	internal_capacity = c_int_capacity;

	Setting& c_ext_capacity = readConfigFile(cfg, 
			"minni.aggregator.hashtable_external.capacity.external");
	external_capacity = c_ext_capacity;

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;

	Setting& c_ehtname = readConfigFile(cfg, 
			"minni.aggregator.hashtable_external.file");
	string htname = (const char*)c_ehtname;

	Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.bucket.aggregate");
	int agg_in_mem = c_agginmem;

	Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
	size_t token_size = c_token_size;

	Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
	size_t max_keys_per_token = c_max_keys;

	Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
	string inp_type = (const char*)c_inp_typ;

	curr_hash = (leveldb::DB**)malloc(sizeof(leveldb::DB*));
	prev_hash = (leveldb::DB**)malloc(sizeof(leveldb::DB*));

	if (type == Map) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
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

		string prev_htname = htname + ".prev";
		hash_reader = new ExternalHashReader(this, prev_hash, 
				prev_htname.c_str(), external_capacity, 
				createPAOFunc, max_keys_per_token);
		creator = new PAOCreator(this, createPAOFunc,
				max_keys_per_token);
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

	ext_hasher = new ExternalHasher(this, curr_hash, htname.c_str(),
			external_capacity, destroyPAOFunc, max_keys_per_token);
	pipeline_list[0].add_filter(*ext_hasher);

	/* Second pipeline: In this pipeline, a token is a fixed number of 
	 * PAOs read from the external hash table. The PAOs are partitioned
	 * for the reducer. */

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	ehash_serializer =  new ExternalHashSerializer(this, curr_hash,
			htname.c_str(), external_capacity, final_path);
	pipeline_list[1].add_filter(*ehash_serializer);
	free(final_path);
}

IterAggregator::~IterAggregator()
{
	if (chunkreader)
		delete(chunkreader);
	if (filereader)
		delete(filereader);
	if (toker)
		delete toker;
	if (inp_deserializer)
		delete inp_deserializer;
	if (hash_reader)
		delete hash_reader;
	if (creator)
		delete creator;
	delete ext_hasher;
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}

bool IterAggregator::repeatPipeline(uint64_t it)
{
	return false;
}
