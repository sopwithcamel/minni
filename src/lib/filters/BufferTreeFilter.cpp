#include "BufferTreeFilter.h"

BufferTreeInserter::BufferTreeInserter(Aggregator* agg,
        Accumulator* acc,
		void (*destroyPAOFunc)(PartialAgg* p),
		const size_t max_keys) :
    filter(/*serial=*/true),
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
    filter(/*serial=*/true),
    AccumulatorInserter(agg, acc, createPAOFunc, max_keys)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

BufferTreeReader::~BufferTreeReader()
{
    delete accumulator_;
	delete send;
}

void* BufferTreeReader::operator()(void* recv)
{
}

BufferTreeSerializer::BufferTreeSerializer(Aggregator* agg,
        Accumulator* acc,
        const char* outfile_prefix) :
    filter(serial_in_order),
    AccumulatorSerializer(agg, acc, outfile_prefix)
{
	char num[10];

	/* Set up files for writing key value pairs to */
	char* fname = (char*)malloc(FILENAME_LENGTH);
	uint64_t n_part = aggregator->getNumPartitions();
	fl = (FILE**)malloc(n_part * sizeof(FILE*));
	for (int i=0; i<n_part; i++) {
		sprintf(num, "%d", i);
		strcpy(fname, outfile_prefix);
		strcat(fname, num);
		fl[i] = fopen(fname, "w");
	}
	free(fname);
	buf = (char*)malloc(BUF_SIZE);
}

BufferTreeSerializer::~BufferTreeSerializer()
{
	free(fl);
	free(buf);
}

void* BufferTreeSerializer::operator()(void*)
{
	PartialAgg* pao;
	int buc;
	uint32_t key_length;
	bool valid, remove;
	string val;
    buffertree::BufferTree* bt = (buffertree::BufferTree*)accumulator_;
	uint64_t n_part = aggregator->getNumPartitions();
    uint64_t hash;
    PartialAgg* pao;
    createPAO(NULL, &pao);

    while (bt->nextValue(&hash, pao)) {
		uint32_t sum = 0;
		for (int i=0; i<strlen(k); i++)
			sum += k[i];
		buc = sum % n_part;
		if (buc < 0)
			buc += n_part;
        pao->serialize(buf);
		strcat(buf, "\n");
		fwrite(buf, 1, strlen(buf), fl[buc]);
	}
	fprintf(stderr, "Closing files\n");
	for (int i=0; i<n_part; i++)
		fclose(fl[i]);
	return NULL;
}
