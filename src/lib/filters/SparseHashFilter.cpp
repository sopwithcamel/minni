#include "SparseHashFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

SparseHashInserter::SparseHashInserter(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
		void (*destroyPAOFunc)(PartialAgg* p),
		size_t max_keys) :
    AccumulatorInserter(agg, acc, destroyPAOFunc, max_keys),
    createPAO_(createPAOFunc),
    next_buffer(0)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
    for (uint32_t j=0; j<num_buffers; j++) {
        for (uint64_t i=0; i<max_keys_per_token; i++) {
            createPAO_(NULL, &((*evicted_list_)[j][i]));
        }
    }
}

SparseHashInserter::~SparseHashInserter()
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
    for (uint32_t j=0; j<num_buffers; j++) {
        for (uint64_t i=0; i<max_keys_per_token; i++) {
            destroyPAO((*evicted_list_)[j][i]);
        }
    }
	delete evicted_list_;
	delete send_;
}

void* SparseHashInserter::operator()(void* recv)
{
	char *key;
	size_t ind = 0;
    uint64_t hashv;
    void* ptrToHash;
    uint64_t bkt;
	PartialAgg* pao;
    SparseHash* sh = (SparseHash*)accumulator_;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    size_t numEvicted = 0;
    size_t evict_list_ctr = 0;

	// Insert PAOs
    if (recv_length > 0) {
        while (ind < recv_length) {
            pao = pao_l[ind];
            Hash(pao->key, strlen(pao->key), NUM_BUCKETS, hashv, bkt); 
            ptrToHash = (void*)&hashv;
            sh->insert(ptrToHash, pao, this_list, numEvicted);
            evict_list_ctr += numEvicted;
            if (recv_list->destroy_pao)
                destroyPAO(pao);
            ind++;
        }
        tokens_processed++;
    }
    assert(evict_list_ctr < max_keys_per_token);
	if (aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens) {
        uint64_t hash;
        void* ptrToHash = (void*)&hash;
        bool remain;
        while(sh->nextValue(ptrToHash, this_list[evict_list_ctr++])) {
            if (evict_list_ctr == max_keys_per_token) {
                aggregator_->voteTerminate = false; // i'm not done yet!
                break;
            }
        }
	}

    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    return this_send;
}

SparseHashReader::SparseHashReader(Aggregator* agg, 
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const size_t max_keys) :
    AccumulatorReader(agg, acc, createPAOFunc, max_keys)
{
}

SparseHashReader::SparseHashReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const char* outfile_prefix) :
    AccumulatorReader(agg, acc, createPAOFunc, outfile_prefix)
{
}

SparseHashReader::~SparseHashReader()
{
}

void* SparseHashReader::operator()(void* recv)
{
    if (writeToFile_) {
        PartialAgg* pao;
        uint64_t buc;
        bool valid, remove;
        string val;
        SparseHash* bt = (SparseHash*)accumulator_;
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
