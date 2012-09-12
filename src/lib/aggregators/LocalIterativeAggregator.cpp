#include "LocalIterativeAggregator.h"

/*
 * Initialize pipeline
 */
LocalIterativeAggregator::LocalIterativeAggregator(const Config &cfg,
                JobID jid,
                AggType type, 
                const uint64_t num_part,
                MapInput* _map_input,
                const char* infile, 
                Operations* ops,
                const char* outfile):
        Aggregator(cfg, jid, type, 2, num_part, ops),
        inp_deserializer_(NULL),
        final_serializer_(NULL),     
        iter_(0)
{
    /* Set up configuration options */
    Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
    size_t token_size = c_token_size;

    Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
    size_t max_keys_per_token = c_max_keys;

    Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
    string fprefix = (const char*)c_fprefix;

    infile_= (char*)malloc(FILENAME_LENGTH);
    strcpy(infile_, fprefix.c_str());
    strcat(infile_, "input");
    inp_deserializer_ = new Deserializer(this, 1, infile_, max_keys_per_token);
    pipeline_list[0].add_filter(*inp_deserializer_);

    outfile_ = (char*)malloc(FILENAME_LENGTH);
    strcpy(outfile_, fprefix.c_str());
    strcat(outfile_, outfile);
    final_serializer_ = new Serializer(this, getNumPartitions(), outfile_); 
    pipeline_list[0].add_filter(*final_serializer_);
}

LocalIterativeAggregator::~LocalIterativeAggregator()
{
    free(infile_);
    free(outfile_);
    delete(inp_deserializer_);
    delete final_serializer_;
}

void LocalIterativeAggregator::runPipeline()
{
	for (int i=0; i<3; i++) {
        fprintf(stderr, "Running the local-iterator pipeline (%d)\n", iter_++);
        pipeline_list[0].run(num_buffers);
        resetFlags();
	}
    pipeline_list[0].clear();
    TimeLog::addTimeStamp(jobid, "Pipeline completed");
}

