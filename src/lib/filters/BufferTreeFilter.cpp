#include "BufferTreeFilter.h"

BufferTreeInserter::BufferTreeInserter(Aggregator* agg,
        Accumulator* acc,
		void (*destroyPAOFunc)(PartialAgg* p),
		size_t max_keys) :
    AccumulatorInserter(agg, acc, destroyPAOFunc, max_keys)
{
}

BufferTreeInserter::~BufferTreeInserter()
{
}

void* BufferTreeInserter::operator()(void* recv)
{
	char *key;
	char* buf = (char*)malloc(BUF_SIZE);
	string value;
	size_t ind = 0;
	PartialAgg* pao;
    buffertree::BufferTree* bt = (buffertree::BufferTree*)accumulator_;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;

	fprintf(stderr, "Got %ld elements to exthash\n", recv_length);

	// Insert PAOs
	while (ind < recv_length) {
		pao = pao_l[ind];
        bt->insert(pao->key, pao);
		destroyPAO(pao);
		ind++;
	}
	free(buf);
}

BufferTreeReader::BufferTreeReader(Aggregator* agg, 
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const size_t max_keys) :
    AccumulatorReader(agg, acc, createPAOFunc, max_keys)
{
}

BufferTreeReader::BufferTreeReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const char* outfile_prefix) :
    AccumulatorReader(agg, acc, createPAOFunc, outfile_prefix)
{
}

BufferTreeReader::~BufferTreeReader()
{
}

void* BufferTreeReader::operator()(void* recv)
{
    if (writeToFile_) {
        PartialAgg* pao;
        uint64_t buc;
        bool valid, remove;
        string val;
        buffertree::BufferTree* bt = (buffertree::BufferTree*)accumulator_;
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
