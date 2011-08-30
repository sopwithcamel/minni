#include "config.h"
#include "HashAggregator.h"

/*
 * Initialize pipeline
 */
HashAggregator::HashAggregator(Config* cfg,
				const uint64_t type, // where to read from
				const uint64_t _partid, 
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, 1, _partid, createPAOFunc, destroyPAOFunc),
		type(DFS_CHUNK_INPUT),
		map_input(_map_input),
		infile(infile),
		outfile(outfile)
{
	Setting& c_empty_key = cfg->lookup("minni.key.empty");
	string empty_key = c_empty_key;
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	Setting& c_capacity = cfg->lookup("aggregator.hashtable_internal.capacity");
	capacity = c_capacity;

	Setting& c_fprefix = cfg->lookup("minni.file_prefix");
	string fprefix = c_fprefix;

	if (DFS_CHUNK_INPUT == type) {
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (LOCAL_PAO_INPUT == type) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		deserializer = new Deserializer(this, 1, input_file,
			emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*deserializer);
		free(input_file);
	}

	hasher = new Hasher<char*, CharHash, eqstr>(this, emptyPAO, 
		destroyPAOFunc);
	if (LOCAL_PAO_INPUT == type)
		hasher->setFlushOnComplete();
	pipeline_list[0].add_filter(*hasher);

	// TODO: Handle output to DFS here
	char* output_file = (char*)malloc(FILENAME_LENGTH);
	strcpy(output_file, fprefix.c_str());
	strcat(output_file, outfile);
	serializer = new Serializer(this, emptyPAO, 1, output_file, 
		destroyPAOFunc);
	pipeline_list[0].add_filter(*serializer);
	free(output_file);
}

HashAggregator::~HashAggregator()
{
	pipeline_list[0].clear();
}
