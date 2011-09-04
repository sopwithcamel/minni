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
	string empty_key;
	string fprefix;

	try {
		Setting& c_empty_key = cfg->lookup("minni.common.key.empty");
		empty_key = (const char*)c_empty_key;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}		
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	try {
		Setting& c_capacity = cfg->lookup("minni.aggregator.hashtable_internal.capacity");
		capacity = c_capacity;
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
		reader = new DFSReader(this, map_input);
		pipeline_list[0].add_filter(*reader);

		toker = new Tokenizer(this, emptyPAO, createPAOFunc);
		pipeline_list[0].add_filter(*toker);
	} else if (LOCAL_PAO_INPUT == type) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		deserializer = new Deserializer(this, 1, input_file,
			emptyPAO, createPAOFunc, destroyPAOFunc);
		pipeline_list[0].add_filter(*deserializer);
		free(input_file);
	}

	hasher = new Hashtable(this, emptyPAO, capacity, destroyPAOFunc);
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
