#include "BucketAggregator.h"

/*
 * Initialize pipeline
 */
BucketAggregator::BucketAggregator(const Config &cfg,
                JobID jid,
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                size_t (*createPAOFunc)(Token* t, PartialAgg** p), 
                void (*destroyPAOFunc)(PartialAgg* p), 
                const char* outfile):
        Aggregator(cfg, jid, type, 2, num_part, createPAOFunc, destroyPAOFunc),
        map_input(_map_input),
        chunkreader(NULL),
        filereader(NULL),
        filetoker(NULL),
        toker(NULL),
        infile_(infile),
        inp_deserializer_(NULL),
        creator_(NULL),
        hasher_(NULL),
        bucket_serializer_(NULL),
        deserializer_(NULL),
        acc_int_inserter_(NULL),
        bucket_inserter_(NULL),
        bucket_hasher_(NULL),
        final_serializer_(NULL),     
        outfile_(outfile)
{
    /* Set up configuration options */
    Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
    size_t token_size = c_token_size;

    Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
    size_t max_keys_per_token = c_max_keys;

    Setting& c_capacity = readConfigFile(cfg, "minni.aggregator.bucket.capacity");
    capacity = c_capacity;

	Setting& c_intagg = readConfigFile(cfg, "minni.internal.selected");
	string intagg = c_intagg;

    Setting& c_nb = readConfigFile(cfg, "minni.aggregator.bucket.num");
    num_buckets = c_nb;

    Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
    string fprefix = (const char*)c_fprefix;

    Setting& c_agginmem = readConfigFile(cfg, "minni.aggregator.bucket.aggregate");
    int agg_in_mem = c_agginmem;

    Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
    string inp_type = (const char*)c_inp_typ;

    /* Initialize data structures */
    if (!intagg.compare("cbt")) {
        Setting& c_fanout = readConfigFile(cfg,
                "minni.internal.cbt.fanout");
        uint32_t fanout = c_fanout;
        Setting& c_buffer_size = readConfigFile(cfg,
                "minni.internal.cbt.buffer_size");
        uint32_t buffer_size = c_buffer_size;
        Setting& c_pao_size = readConfigFile(cfg,
                "minni.internal.cbt.pao_size");
        uint32_t pao_size = c_pao_size;
        acc_internal_ = dynamic_cast<Accumulator*>(new 
                compresstree::CompressTree(2, fanout, 1000, buffer_size, pao_size,
                createPAOFunc, destroyPAOFunc));
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                CompressTreeInserter(this, acc_internal_,
                HashUtil::MURMUR, createPAOFunc,
                destroyPAOFunc, max_keys_per_token));
        bucket_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                CompressTreeInserter(this, acc_internal_,
                HashUtil::BOB, createPAOFunc,
                destroyPAOFunc, max_keys_per_token));

    } else if (!intagg.compare("sparsehash")) {
        Setting& c_num_part = readConfigFile(cfg,
                "minni.internal.sparsehash.partitions");
        int num_part = c_num_part;
        acc_internal_ = dynamic_cast<Accumulator*>(new SparseHashMurmur(capacity,
                max_keys_per_token));
        acc_bucket_ = dynamic_cast<Accumulator*>(new SparseHashBob(capacity,
                max_keys_per_token));
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                SparseHashInserter(this, acc_internal_, createPAOFunc,
                destroyPAOFunc, num_part, max_keys_per_token));
        bucket_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                SparseHashInserter(this, acc_bucket_, createPAOFunc,
                destroyPAOFunc, 1, max_keys_per_token));

    } 
#ifdef UTHASH
    else if (!intagg.compare("uthash")) {
        hashtable_ = dynamic_cast<Hashtable*>(new UTHashtable(capacity));
    }
#endif

    if (type == Map) {
        /* Beginning of first pipeline: this pipeline takes the entire
         * entire input, chunk by chunk, tokenizes, Maps each Minni-
         * token aggregates/writes to buckets. For this pipeline, a 
         * "token" or a basic pipeline unit is a chunk read from the 
         * DFS */
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
        }

        creator_ = new PAOCreator(this, createPAOFunc, max_keys_per_token);
        pipeline_list[0].add_filter(*creator_);

    } else if (type == Reduce) {
        char* input_file = (char*)malloc(FILENAME_LENGTH);
        strcpy(input_file, fprefix.c_str());
        strcat(input_file, infile_);
        inp_deserializer_ = new Deserializer(this, 1, input_file,
            createPAOFunc, destroyPAOFunc, max_keys_per_token);
        pipeline_list[0].add_filter(*inp_deserializer_);
        free(input_file);
    }

    if (agg_in_mem) {
        if (!intagg.compare("cbt") || !intagg.compare("sparsehash")) {
            pipeline_list[0].add_filter(*acc_int_inserter_);
        } else {
            hasher_ = new Hasher(this, hashtable_, destroyPAOFunc,
                    max_keys_per_token);
            pipeline_list[0].add_filter(*hasher_);
        }
    }

    char* bucket_prefix = (char*)malloc(FILENAME_LENGTH);
    strcpy(bucket_prefix, fprefix.c_str());
    char jidstr[10];
    sprintf(jidstr, "%lu", (uint64_t)jid);
    strcat(jidstr, "-");
    strcat(bucket_prefix, jidstr);
    if (type == Map)
        strcat(bucket_prefix, "map-");
    else if (type == Reduce)
        strcat(bucket_prefix, "reduce-");
    strcat(bucket_prefix, "bucket");

    bucket_serializer_ = new Serializer(this, num_buckets, 
            bucket_prefix, createPAOFunc, destroyPAOFunc);
    pipeline_list[0].add_filter(*bucket_serializer_);
    
    /* Second pipeline: In this pipeline, a token is an entire bucket. In
     * other words, each pipeline stage is called once for each bucket to
     * be processed. This may not be fine-grained enough, but should have
     * enough parallelism to keep our wimpy-node busy. 

     * In this pipeline, a bucket is read back into memory (converted to 
     * PAOs again), aggregated using a hashtable, and serialized. */
    deserializer_ = new Deserializer(this, num_buckets, bucket_prefix,
            createPAOFunc, destroyPAOFunc, max_keys_per_token);
    pipeline_list[1].add_filter(*deserializer_);

    if (!intagg.compare("cbt") || !intagg.compare("sparsehash")) {
        pipeline_list[1].add_filter(*bucket_inserter_);
    } else {
        bucket_hasher_ = new Hasher(this, hashtable_, destroyPAOFunc,
                max_keys_per_token);
        pipeline_list[1].add_filter(*bucket_hasher_);
    }

    char* final_path = (char*)malloc(FILENAME_LENGTH);
    strcpy(final_path, fprefix.c_str());
    strcat(final_path, outfile_);
    final_serializer_ = new Serializer(this, getNumPartitions(), 
            final_path, createPAOFunc, destroyPAOFunc); 
    pipeline_list[1].add_filter(*final_serializer_);

    free(bucket_prefix);
    free(final_path);
}

BucketAggregator::~BucketAggregator()
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
    if (hasher_) {
        delete hashtable_;
        delete hasher_;
    }
    if (acc_int_inserter_) {
        delete acc_internal_;
        delete acc_int_inserter_;
    }
    if (bucket_serializer_)
        delete bucket_serializer_;
    if (deserializer_)
        delete deserializer_;
    if (bucket_hasher_)
        delete bucket_hasher_;
    if (bucket_inserter_) {
//        delete acc_bucket_;
        delete bucket_inserter_;
    }
    if (final_serializer_)
        delete final_serializer_;
}
