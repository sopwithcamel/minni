#include "config.h"
#include "ExthashAggregator.h"

/*
 * Initialize pipeline
 */
ExthashAggregator::ExthashAggregator(Config* cfg,
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
	Setting& c_empty_key = cfg->lookup("minni.key.empty");
	string empty_key = c_empty_key;
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	Setting& c_int_capacity = cfg->lookup(
			"aggregator.hashtable_external.capacity.internal");
	internal_capacity = c_int_capacity;

	Setting& c_ext_capacity = cfg->lookup(
			"aggregator.hashtable_external.capacity.external");
	external_capacity = c_ext_capacity;

	Setting& c_fprefix = cfg->lookup("minni.file_prefix");
	string fprefix = c_fprefix;

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

	hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO,
			destroyPAOFunc);
	if (LOCAL_PAO_INPUT == type)
		hasher->setFlushOnComplete();
	pipeline_list[0].add_filter(*hasher);

	char* ht_name = (char*)malloc(FILENAME_LENGTH);
	strcpy(ht_name, fprefix.c_str());
	strcat(ht_name, "hashtable_dump");
	ext_hasher = new ExternalHasher(this, ht_name, emptyPAO, destroyPAOFunc);
	pipeline_list[0].add_filter(*ext_hasher);

	free(ht_name);
}

ExthashAggregator::~ExthashAggregator()
{
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
