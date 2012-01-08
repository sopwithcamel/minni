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
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

BufferTreeReader::~BufferTreeReader()
{
    delete accumulator_;
	delete send;
}

void* BufferTreeReader::operator()(void* recv)
{
	PartialAgg* pao;
	int buc;
	uint32_t key_length;
	bool valid, remove;
	string val;
    buffertree::BufferTree* bt = (buffertree::BufferTree*)accumulator_;
	uint64_t n_part = aggregator_->getNumPartitions();
    uint64_t hash;
    createPAO(NULL, &pao);
    void* ptrToHash = (void*)&hash;
    while (bt->nextValue(ptrToHash, pao)) {
	}
	return NULL;
}
