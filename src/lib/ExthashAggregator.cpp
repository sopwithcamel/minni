#include "ExthashAggregator.h"

/*
 * Initialize pipeline
 */
ExthashAggregator::ExthashAggregator(const Config& cfg,
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

	hasher = new Hasher(this, emptyPAO, internal_capacity, destroyPAOFunc);
	pipeline_list[0].add_filter(*hasher);

	ext_hasher = new ExternalHasher(this, htname.c_str(), external_capacity, 
			emptyPAO, destroyPAOFunc);
	pipeline_list[0].add_filter(*ext_hasher);


	/* Second pipeline: In this pipeline, a token is a fixed number of 
	 * PAOs read from the external hash table. The PAOs are partitioned
	 * for the reducer. */

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	eh_reader = new ExthashReader(this, htname.c_str(), external_capacity, 
			emptyPAO, final_path);
	pipeline_list[1].add_filter(*eh_reader);
	free(final_path);
}

ExthashAggregator::~ExthashAggregator()
{
	if (reader)
		delete reader;
	if (toker)
		delete toker;
	if (inp_deserializer)
		delete inp_deserializer;
	delete hasher;
	delete ext_hasher;
	delete eh_reader;
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
