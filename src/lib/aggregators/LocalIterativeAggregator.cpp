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
        infile_(infile),
        inp_deserializer_(NULL),
        final_serializer_(NULL),     
        outfile_(outfile)
{
    /* Set up configuration options */
    Setting& c_token_size = readConfigFile(cfg, "minni.tbb.token_size");
    size_t token_size = c_token_size;

    Setting& c_max_keys = readConfigFile(cfg, "minni.tbb.max_keys_per_token");
    size_t max_keys_per_token = c_max_keys;

    Setting& c_fprefix = readConfigFile(cfg, "minni.common.file_prefix");
    string fprefix = (const char*)c_fprefix;

    char* input_file = (char*)malloc(FILENAME_LENGTH);
    strcpy(input_file, fprefix.c_str());
    strcat(input_file, "input");
    inp_deserializer_ = new Deserializer(this, 1, input_file,
            max_keys_per_token);
    pipeline_list[0].add_filter(*inp_deserializer_);
    free(input_file);

    char* final_path = (char*)malloc(FILENAME_LENGTH);
    strcpy(final_path, fprefix.c_str());
    strcat(final_path, outfile_);
    final_serializer_ = new Serializer(this, getNumPartitions(), 
            final_path); 
    pipeline_list[0].add_filter(*final_serializer_);

    free(final_path);
}

LocalIterativeAggregator::~LocalIterativeAggregator()
{
    if (inp_deserializer_)
        delete(inp_deserializer_);
    if (final_serializer_)
        delete final_serializer_;
}

void LocalIterativeAggregator::runPipeline()
{
	for (int i=0; i<num_pipelines; i++) {
        fprintf(stderr, "Running the local-iterator pipeline %d\n", i);
        pipeline_list[i].run(num_buffers);
        resetFlags();
        pipeline_list[i].clear();
		TimeLog::addTimeStamp(jobid, "Pipeline completed");
	}
}

