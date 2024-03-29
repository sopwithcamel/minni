#include "ExthashAggregator.h"
#include "CompressTreeFilter.h"
#include "ConcurrentHashFilter.h"
#include "SparseHashFilter.h"

/*
 * Initialize pipeline
 */
ExthashAggregator::ExthashAggregator(const Config &cfg,
                JobID jid,
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                Operations* ops,
                const char* outfile):
        Aggregator(cfg, jid, type, 2, num_part, ops),
        map_input(_map_input),
        chunkreader(NULL),
        filereader(NULL),
        filetoker(NULL),
        toker(NULL),
        infile_(infile),
        inp_deserializer_(NULL),
        creator_(NULL),
        acc_int_inserter_(NULL),
        final_serializer_(NULL),     
        outfile_(outfile)
{
    /* Set up configuration options */
    Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
    size_t token_size = c_token_size;

    Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
    size_t max_keys_per_token = c_max_keys;

	Setting& c_intagg = readConfigFile(cfg, "minni.internal.selected");
	string intagg = c_intagg;

    Setting& c_agginmem = readConfigFile(cfg, "minni.internal.enabled");
    int agg_in_mem = c_agginmem;

    Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
    string fprefix = (const char*)c_fprefix;

    Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
    string inp_type = (const char*)c_inp_typ;

	Setting& c_ehtname = readConfigFile(cfg, "minni.aggregator.exthash.file");
	string htname = (const char*)c_ehtname;

    /* Initialize data structures */
    if (!intagg.compare("cbt")) {
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                CompressTreeInserter(this, cfg,
                HashUtil::MURMUR, max_keys_per_token));

    } else if (!intagg.compare("sparsehash")) {
        Setting& c_num_part = readConfigFile(cfg,
                "minni.internal.sparsehash.partitions");
        int num_part = c_num_part;
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                SparseHashInserter(this, cfg, num_part, max_keys_per_token));
    } 

    if (type == Map) {
        if (!inp_type.compare("chunk")) { 
            chunkreader = new DFSReader(this, map_input, 
                    token_size);
            pipeline_list[0].add_filter(*chunkreader);
            toker = new TokenizerFilter(this, cfg,
                    max_keys_per_token);
            pipeline_list[0].add_filter(*toker);
        } else if (!inp_type.compare("file")) {
            filereader = new FileReaderFilter(this, map_input);
            pipeline_list[0].add_filter(*filereader);
            filetoker = new FileTokenizerFilter(this, cfg,
                    map_input, max_keys_per_token);
            pipeline_list[0].add_filter(*filetoker);
        } else if (!inp_type.compare("filechunk")) {
            filechunker = new FileChunkerFilter(this, map_input, cfg,
                    max_keys_per_token);
            pipeline_list[0].add_filter(*filechunker);
        } else if (!inp_type.compare("local")) {
            localreader_ = new LocalReader(this, fprefix.c_str(),
                    max_keys_per_token);
            pipeline_list[0].add_filter(*localreader_);
        }

        if (!intagg.compare("cbt")) {
            creator_ = new PAOCreator(this, max_keys_per_token,
                    /*refresh_paos=*/true);
        } else {
            creator_ = new PAOCreator(this, max_keys_per_token,
                    /*refresh_paos=*/false);
        }
        pipeline_list[0].add_filter(*creator_);

    } else if (type == Reduce) {
        char* input_file = (char*)malloc(FILENAME_LENGTH);
        strcpy(input_file, fprefix.c_str());
        strcat(input_file, infile_);
        inp_deserializer_ = new Deserializer(this, 1, input_file,
                max_keys_per_token);
        pipeline_list[0].add_filter(*inp_deserializer_);
        free(input_file);
    }

    if (agg_in_mem) {
        if (!intagg.compare("cbt") || !intagg.compare("sparsehash")) {
            pipeline_list[0].add_filter(*acc_int_inserter_);
        }
    }

    ext_hasher_ = new ExternalHasher(this, htname.c_str(),
            max_keys_per_token);
    pipeline_list[0].add_filter(*ext_hasher_);

    char* final_path = (char*)malloc(FILENAME_LENGTH);
    strcpy(final_path, fprefix.c_str());
    strcat(final_path, outfile_);
    final_serializer_ = new Serializer(this, getNumPartitions(), final_path); 
    pipeline_list[0].add_filter(*final_serializer_);

    
    free(final_path);
}

ExthashAggregator::~ExthashAggregator()
{
    if (chunkreader)
        delete(chunkreader);
    if (filereader)
        delete(filereader);
    if (toker)
        delete(toker);
    if (inp_deserializer_)
        delete(inp_deserializer_);
    delete creator_;
    if (acc_int_inserter_) {
        delete acc_int_inserter_;
    }
    delete ext_hasher_;
    if (final_serializer_)
        delete final_serializer_;
}
