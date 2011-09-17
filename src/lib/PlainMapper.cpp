#include "PlainMapper.h"

/*
 * Initialize pipeline
 */
PlainMapper::PlainMapper(const Config &cfg,
				AggType type, 
				const uint64_t num_part,
				MapInput* _map_input,
				const char* infile, 
				PartialAgg* (*createPAOFunc)(const char* t), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 2, num_part, createPAOFunc, destroyPAOFunc),
		map_input(_map_input),
		outfile(outfile)
{
	Setting& c_empty_key = readConfigFile(cfg, "minni.common.key.empty");
	string empty_key = (const char*)c_empty_key;
	PartialAgg* emptyPAO = createPAOFunc(empty_key.c_str());

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;


	// Can't do this on the the reduce side
	assert(type == Map);
	/* Beginning of first pipeline: this pipeline takes the entire
	 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
	 * and serializes to file. For this pipeline, a "token" or a
	 * a basic pipeline unit is part of a chunk read from the DFS */
	reader = new DFSReader(this, map_input);
	pipeline_list[0].add_filter(*reader);

	toker = new Tokenizer(this, emptyPAO, createPAOFunc);
	pipeline_list[0].add_filter(*toker);

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	final_serializer = new Serializer(this, emptyPAO, getNumPartitions(), 
			final_path, destroyPAOFunc);
	pipeline_list[0].add_filter(*final_serializer);

	free(final_path);
}

PlainMapper::~PlainMapper()
{
	delete(reader);
	delete(toker);
	delete(final_serializer);
	pipeline_list[0].clear();
}
