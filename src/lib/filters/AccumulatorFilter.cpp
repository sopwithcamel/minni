#include "AccumulatorFilter.h"

static const size_t BUF_SIZE = 65535;

AccumulatorInserter::AccumulatorInserter(Aggregator* agg,
        Accumulator* acc,
        void (*destroyPAOFunc)(PartialAgg* p),
        const size_t max_keys) :
    filter(/*serial=*/true),
    aggregator_(agg),
    accumulator_(acc),
    destroyPAO(destroyPAOFunc),
    max_keys_per_token(max_keys)
{
}


AccumulatorInserter::~AccumulatorInserter()
{
}


AccumulatorReader::AccumulatorReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const size_t max_keys) :
    filter(/*serial=*/true),
    aggregator_(agg),
    accumulator_(acc),
    createPAO_(createPAOFunc),
    max_keys_per_token_(max_keys),
    writeToFile_(false),
    fl_(NULL),
    outfile_(NULL),
    buf_(NULL)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
    /* TODO: Allocate pao_list_ */
}
    
AccumulatorReader::AccumulatorReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const char* outfile_prefix) :
    filter(/*serial=*/true),
    aggregator_(agg),
    accumulator_(acc),
    createPAO_(createPAOFunc),
    max_keys_per_token_(0),
    writeToFile_(true),
    send_(NULL),
    pao_list_(NULL)
{
	/* Set up files for writing key value pairs to */
	char num[10];
	char* fname = (char*)malloc(FILENAME_LENGTH);
	uint64_t n_part = aggregator_->getNumPartitions();
	fl_ = (FILE**)malloc(n_part * sizeof(FILE*));
	for (int i=0; i<n_part; i++) {
		sprintf(num, "%d", i);
		strcpy(fname, outfile_prefix);
		strcat(fname, num);
		fl_[i] = fopen(fname, "w");
	}
	fprintf(stderr, "Serializing to file: %s\n", fname);
	free(fname);
	buf_ = (char*)malloc(BUF_SIZE);
}

AccumulatorReader::~AccumulatorReader()
{
    if (writeToFile_) {
        free(fl_);
        free(buf_);
    } else {
        delete send_;
    }
}
