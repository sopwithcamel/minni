#include "BucketAggregator.h"
#include "CompressTreeFilter.h"
#include "ConcurrentHashFilter.h"
#include "SparseHashFilter.h"

/*
 * Initialize pipeline
 */
BucketAggregator::BucketAggregator(const Config &cfg,
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
        bucket_serializer_(NULL),
        deserializer_(NULL),
        acc_int_inserter_(NULL),
        bucket_inserter_(NULL),
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

    Setting& c_nb = readConfigFile(cfg, "minni.aggregator.bucket.num");
    num_buckets = c_nb;

    Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
    string fprefix = (const char*)c_fprefix;

    Setting& c_inp_typ = readConfigFile(cfg, "minni.input_type");
    string inp_type = (const char*)c_inp_typ;

    if (!intagg.compare("cbt")) {
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                CompressTreeInserter(this, cfg,
                HashUtil::MURMUR, max_keys_per_token));
        bucket_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                CompressTreeInserter(this, cfg,
                HashUtil::BOB, max_keys_per_token));
    } else if (!intagg.compare("sparsehash")) {
        Setting& c_concurrent = readConfigFile(cfg,
                "minni.internal.sparsehash.concurrent");
        int concurrent = c_concurrent;
        Setting& c_num_part = readConfigFile(cfg,
                "minni.internal.sparsehash.partitions");
        int num_part = c_num_part;
        acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                SparseHashInserter(this, cfg,
                num_part, max_keys_per_token));
        if (!concurrent) {
            acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                    SparseHashInserter(this, cfg,
                    num_part, max_keys_per_token));
            bucket_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                    SparseHashInserter(this, cfg,
                    1, max_keys_per_token));
        } else {
            acc_int_inserter_ = dynamic_cast<AccumulatorInserter*>(new
                    ConcurrentHashInserter(this, cfg,
                    max_keys_per_token));
            bucket_inserter_ = dynamic_cast<AccumulatorInserter*>(new 
                    SparseHashInserter(this, cfg,
                    num_part, max_keys_per_token));
        }
    } 

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
        } else if (!inp_type.compare("filechunk")) {
            filechunker = new FileChunkerFilter(this, map_input, cfg,
                    max_keys_per_token);
            pipeline_list[0].add_filter(*filechunker);
        } else if (!inp_type.compare("local")) {
            localreader_ = new LocalReader(this, fprefix.c_str(),
                    max_keys_per_token);
            pipeline_list[0].add_filter(*localreader_);
        }

        creator_ = new PAOCreator(this, max_keys_per_token);
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

    bucket_serializer_ = new Serializer(this, num_buckets, bucket_prefix);
    pipeline_list[0].add_filter(*bucket_serializer_);
    
    /* Second pipeline: In this pipeline, a token is an entire bucket. In
     * other words, each pipeline stage is called once for each bucket to
     * be processed. This may not be fine-grained enough, but should have
     * enough parallelism to keep our wimpy-node busy. 

     * In this pipeline, a bucket is read back into memory (converted to 
     * PAOs again), aggregated using a hashtable, and serialized. */
    deserializer_ = new Deserializer(this, num_buckets, bucket_prefix,
            max_keys_per_token);
    pipeline_list[1].add_filter(*deserializer_);

    if (!intagg.compare("cbt") || !intagg.compare("sparsehash")) {
        pipeline_list[1].add_filter(*bucket_inserter_);
    }

    char* final_path = (char*)malloc(FILENAME_LENGTH);
    strcpy(final_path, fprefix.c_str());
    strcat(final_path, outfile_);
    final_serializer_ = new Serializer(this, getNumPartitions(), 
            final_path); 
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
    if (acc_int_inserter_) {
        delete acc_int_inserter_;
    }
    if (bucket_serializer_)
        delete bucket_serializer_;
    if (deserializer_)
        delete deserializer_;
    if (bucket_inserter_) {
//        delete acc_bucket_;
        delete bucket_inserter_;
    }
    if (final_serializer_)
        delete final_serializer_;
}
