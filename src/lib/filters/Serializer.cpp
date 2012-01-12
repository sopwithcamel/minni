#include "Serializer.h"

#define BUF_SIZE	65535

Serializer::Serializer(Aggregator* agg,
			const uint64_t nb, 
			const char* outfile_prefix,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(serial_in_order),
		aggregator(agg),
		already_partitioned(false),
		num_buckets(nb),
		tokens_processed(0),
		destroyPAO(destroyPAOFunc)
{
	char num[10];
	char* fname = (char*)malloc(FILENAME_LENGTH);

	fl = (FILE**)malloc(num_buckets * sizeof(FILE*));

	for (int i=0; i<num_buckets; i++) {
		sprintf(num, "%d", i);
		strcpy(fname, outfile_prefix);
		strcat(fname, num);

		fl[i] = fopen(fname, "w");
		assert(NULL != fl[i]);
	}
	free(fname);
	type = aggregator->getType();
	buf = malloc(BUF_SIZE);	
}

Serializer::~Serializer()
{
	free(fl);
	free(buf);
}

int Serializer::partition(const char* key)
{
	int buc, sum = 0;
	for (int i=0; i<strlen(key); i++)
		sum += key[i];
	buc = (sum + type) % num_buckets;
	if (buc < 0)
		buc += num_buckets;
	return buc;
}

void* Serializer::operator()(void* pao_list)
{
	PartialAgg* pao;
	int buc;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;
	uint64_t ind = 0;

	while(ind < recv_length) {
		pao = pao_l[ind];
		buc = partition(pao->key);	
        assert(pao != NULL);
		pao->serialize(fl[buc], buf, BUF_SIZE);
        if (recv->destroy_pao)
    		destroyPAO(pao);
		ind++;
	}
	// reset flags; TODO: why are flags being reset here?
//	aggregator->resetFlags();

	tokens_processed++;
	if (aggregator->input_finished && 
			tokens_processed == aggregator->tot_input_tokens &&
            aggregator->voteTerminate == true) {
		fprintf(stderr, "Closing bucket files\n");

		for (int i=0; i<num_buckets; i++) {
            fprintf(stderr, "Closing file: %d/%d\n", i, num_buckets);
			fclose(fl[i]);
        }

	}
}
