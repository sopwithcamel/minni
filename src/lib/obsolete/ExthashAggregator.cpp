#include "ExthashAggregator.h"

/*
 * Initialize pipeline
 */
ExthashAggregator::ExthashAggregator(const Config& cfg,
				AggType type,
				const uint64_t num_part, 
				MapInput* map_input,
				const char* infile, 
				size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
				void (*destroyPAOFunc)(PartialAgg* p), 
				const char* outfile):
		Aggregator(cfg, type, 2, num_part, createPAOFunc, destroyPAOFunc),
		map_input_(map_input),
		infile_(infile),
		outfile_(outfile)
{
	Setting& c_int_capacity = readConfigFile(cfg, 
			"minni.aggregator.exthash.capacity.internal");
	internal_capacity = c_int_capacity;

	Setting& c_ext_capacity = readConfigFile(cfg, 
			"minni.aggregator.exthash.capacity.external");
	external_capacity = c_ext_capacity;

	Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
	string fprefix = (const char*)c_fprefix;

	Setting& c_ehtname = readConfigFile(cfg, 
			"minni.aggregator.exthash.file");
	string htname = (const char*)c_ehtname;

	Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
	size_t max_keys_per_token = c_max_keys;

	Setting& c_intagg = readConfigFile(cfg, "minni.internal.selected");
	string intagg = c_intagg;

	Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.exthash.aggregate");
	int agg_in_mem = c_agginmem;

	Setting& c_accumtyp = readConfigFile(cfg, "minni.aggregator.exthash.type");
	string accum_type = (const char*)c_accumtyp;

	Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
	string inp_type = (const char*)c_inp_typ;

    char* final_path;

    /* Initialize data structures */
    if (!intagg.compare("comp-bt")) {
        acc_internal_ = dynamic_cast<Accumulator*>(new 
                compresstree::CompressTree(2, 8, 90, createPAOFunc, 
                destroyPAOFunc));
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                CompressTreeInserter(this, acc_internal_, createPAOFunc,
                destroyPAOFunc, max_keys_per_token));
    } else if (!intagg.compare("sparsehash")) {
        acc_internal_ = dynamic_cast<Accumulator*>(new SparseHash(
                internal_capacity));
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                SparseHashInserter(this, acc_internal_, createPAOFunc,
                destroyPAOFunc, max_keys_per_token));
    }
#ifdef UTHASH
    else if (!intagg.compare("uthash")) {
        hashtable_ = dynamic_cast<Hashtable*>(new UTHashtable(
                internal_capacity));
    }
#endif

    if (!accum_type.compare("buffertree")) {
            accumulator_ = dynamic_cast<Accumulator*>(new 
                    buffertree::BufferTree(2, 8, createPAOFunc,
                    destroyPAOFunc));
            acc_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                    BufferTreeInserter(this, accumulator_, 
                    destroyPAOFunc, max_keys_per_token));

            final_path = (char*)malloc(FILENAME_LENGTH);
            strcpy(final_path, fprefix.c_str());
            strcat(final_path, outfile_);
            acc_reader_ =  dynamic_cast<AccumulatorReader*>(new 
                    BufferTreeReader(this, accumulator_, createPAOFunc,
                    final_path));
    }
//    else if (!accum_type.compare("leveldb"))
//          define others

	if (type == Map) {
		/* Beginning of first pipeline: this pipeline takes the entire
		 * entire input, chunk by chunk, tokenizes, Maps each Minni-token,
		 * aggregates/writes to buckets. For this pipeline, a "token" or a
		 * a basic pipeline unit is a chunk read from the DFS */
		if (!inp_type.compare("chunk")) { 
			chunkreader_ = new DFSReader(this, map_input);
			pipeline_list[0].add_filter(*chunkreader_);
			toker_ = new TokenizerFilter(this, cfg, max_keys_per_token);
			pipeline_list[0].add_filter(*toker_);
		} else if (!inp_type.compare("file")) {
			filereader_ = new FileReaderFilter(this, map_input);
			pipeline_list[0].add_filter(*filereader_);
			filetoker_ = new FileTokenizerFilter(this, cfg,
					 map_input, max_keys_per_token);
			pipeline_list[0].add_filter(*filetoker_);
		}

		creator_ = new PAOCreator(this, createPAOFunc, max_keys_per_token);
		pipeline_list[0].add_filter(*creator_);
	} else if (type == Reduce) {
		char* input_file = (char*)malloc(FILENAME_LENGTH);
		strcpy(input_file, fprefix.c_str());
		strcat(input_file, infile);
		inp_deserializer_ = new Deserializer(this, 1/*TODO: how many?*/, input_file,
			createPAOFunc, destroyPAOFunc, max_keys_per_token);
		pipeline_list[0].add_filter(*inp_deserializer_);
		free(input_file);
	}

	if (agg_in_mem) {
        if (!intagg.compare("comp-bt") || !intagg.compare("sparsehash")) {
            pipeline_list[0].add_filter(*acc_int_inserter_);
        } else {
            hasher_ = new Hasher(this, hashtable_, destroyPAOFunc,
                    max_keys_per_token);
            pipeline_list[0].add_filter(*hasher_);
        }
	}

	pipeline_list[0].add_filter(*acc_inserter_);

	/* Second pipeline: In this pipeline, a token is a fixed number of 
	 * PAOs read from the external hash table. The PAOs are partitioned
	 * for the reducer. */

    pipeline_list[1].add_filter(*acc_reader_);
	free(final_path);
}

ExthashAggregator::~ExthashAggregator()
{
	if (chunkreader_)
		delete chunkreader_;
	if (filereader_)
		delete filereader_;
	if (toker_)
		delete toker_;
	if (filetoker_)
		delete filetoker_;
	if (inp_deserializer_)
		delete inp_deserializer_;
	if (hasher_) {
		delete hasher_;
	}
    delete acc_reader_;
    delete hashtable_;
    delete accumulator_;
	pipeline_list[0].clear();
	pipeline_list[1].clear();
}