#include "CompressTreeFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

CompressTreeInserter::CompressTreeInserter(Aggregator* agg,
        Accumulator* acc,
		void (*destroyPAOFunc)(PartialAgg* p),
		size_t max_keys) :
    AccumulatorInserter(agg, acc, destroyPAOFunc, max_keys),
    next_buffer(0)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
}

CompressTreeInserter::~CompressTreeInserter()
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	delete evicted_list_;
	delete send_;
}

void* CompressTreeInserter::operator()(void* recv)
{
	char *key;
	char* buf = (char*)malloc(BUF_SIZE);
	string value;
	size_t ind = 0;
    uint64_t hashv;
    void* ptrToHash;
    uint64_t bkt;
	PartialAgg* pao;
    compresstree::CompressTree* ct = (compresstree::CompressTree*)accumulator_;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    size_t numEvicted = 0;

	// Insert PAOs
	while (ind < recv_length) {
		pao = pao_l[ind];
        Hash(pao->key, strlen(pao->key), NUM_BUCKETS, hashv, bkt); 
        ptrToHash = (void*)&hashv;
        ct->insert(ptrToHash, pao, this_list, numEvicted);
		destroyPAO(pao);
		ind++;
	}
	free(buf);

    this_send->result = this_list;
    this_send->length = numEvicted;
    return this_send;
}

CompressTreeReader::CompressTreeReader(Aggregator* agg, 
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const size_t max_keys) :
    AccumulatorReader(agg, acc, createPAOFunc, max_keys)
{
}

CompressTreeReader::CompressTreeReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const char* outfile_prefix) :
    AccumulatorReader(agg, acc, createPAOFunc, outfile_prefix)
{
}

CompressTreeReader::~CompressTreeReader()
{
}

void* CompressTreeReader::operator()(void* recv)
{
    if (writeToFile_) {
        PartialAgg* pao;
        uint64_t buc;
        bool valid, remove;
        string val;
        compresstree::CompressTree* bt = (compresstree::CompressTree*)accumulator_;
        uint64_t n_part = aggregator_->getNumPartitions();
        uint64_t hash;
        createPAO_(NULL, &pao);
        void* ptrToHash = (void*)&hash;
        while (bt->nextValue(ptrToHash, pao)) {
            buc = *(uint64_t*)ptrToHash % n_part;
            assert(buc >= 0);
            pao->serialize(buf_);
            fwrite(buf_, 1, strlen(buf_), fl_[buc]);
        }
        fprintf(stderr, "Closing files\n");
        for (int i=0; i<n_part; i++)
            fclose(fl_[i]);
        return NULL;
    } else {
        fprintf(stderr, "Not implemented yet!");
        assert(false);
    }
}
