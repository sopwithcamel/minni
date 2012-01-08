#include "ExthashAggregator.h"

/*
 * Initialize pipeline
 */
ExthashAggregator::ExthashAggregator(const Config& cfg,
				AggType type,
				const uint64_t num_part, 
				MapInput* _map_input,
				const char* infile, 
				size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
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

	Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
	size_t max_keys_per_token = c_max_keys;

	Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.hashtable_external.aggregate");
	int agg_in_mem = c_agginmem;

	Setting& c_accumtyp = readConfigFile(cfg, "minni.aggregator.hashtable_external.external.type");
	string accum_type = (const char*)c_accumtyp;

	Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
	string inp_type = (const char*)c_inp_typ;

    /* Initialize data structures */
    hashtable_ = dynamic_cast<Hashtable*>(new UTHashtable(internal_capacity));
    if (!accum_type.compare("buffertree")) {
            accumulator_ = dynamic_cast<Accumulator*>(new 
                    buffertree::BufferTree(2, 8));
            acc_inserter_ = dynamic_cast<AccumulatorFilter*>(new BufferTreeFilter(
                    this, accumulator_, createPAOFunc, destroyPAOFunc, 
                    max_keys_per_token));
            acc_serializer_ =  dynamic_cast<AccumulatorSerializer*>(new 
                    BufferTreeSerializer(this, accumulator_, final_path));
    }
//    else if (!accum_type.compare("leveldb"))
//          define others

	if (type == Map) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
		if (!inp_type.compare("chunk")) { 
			chunkreader = new DFSReader(this, map_input);
			pipeline_list[0].add_filter(*chunkreader);
			toker = new TokenizerFilter(this, cfg);
			pipeline_list[0].add_filter(*toker);
		} else if (!inp_type.compare("file")) {
			filereader = new FileReaderFilter(this, map_input);
			pipeline_list[0].add_filter(*filereader);
			filetoker = new FileTokenizerFilter(this, cfg,
					 map_input, max_keys_per_token);
			pipeline_list[0].add_filter(*filetoker);
		}

		creator = new PAOCreator(this, createPAOFunc);
		pipeline_list[0].add_filter(*creator);
	} else if (type == Reduce) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		inp_deserializer = new Deserializer(this, 1/*TODO: how many?*/, input_file,
			createPAOFunc, destroyPAOFunc);
		pipeline_list[0].add_filter(*inp_deserializer);
		free(input_file);
	}

	if (agg_in_mem) {
		hasher = new Hasher(this, hashtable_, destroyPAOFunc);
		pipeline_list[0].add_filter(*hasher);

		merger = new Merger(this, destroyPAOFunc);
		pipeline_list[0].add_filter(*merger);
	}

	pipeline_list[0].add_filter(*acc_inserter_);

	/* Second pipeline: In this pipeline, a token is a fixed number of 
	 * PAOs read from the external hash table. The PAOs are partitioned
	 * for the reducer. */

	char* final_path = (char*)malloc(FILENAME_LENGTH);
	strcpy(final_path, fprefix.c_str());
	strcat(final_path, outfile);
	pipeline_list[1].add_filter(*acc_serializer_);
	free(final_path);
}

ExthashAggregator::~ExthashAggregator()
{
	if (chunkreader)
		delete chunkreader;
	if (filereader)
		delete filereader;
	if (toker)
		delete toker;
	if (filetoker)
		delete filetoker;
	if (inp_deserializer)
		delete inp_deserializer;
	if (hasher) {
		delete hasher;
		delete merger;
	}
    delete hashtable_;
    delete accumulator_;
	delete ext_hasher;
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}
