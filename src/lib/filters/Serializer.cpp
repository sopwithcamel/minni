#include "Serializer.h"

Serializer::Serializer(Aggregator* agg,
			PartialAgg* emptyPAO,
			const uint64_t nb, 
			const char* outfile_prefix,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(serial_in_order),
		aggregator(agg),
		emptyPAO(emptyPAO),
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
}

Serializer::~Serializer()
{
	free(fl);
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
		pao->serialize(fl[buc]);
		destroyPAO(pao);
		ind++;
	}
	// reset flags; TODO: why are flags being reset here?
//	aggregator->resetFlags();

	tokens_processed++;
	if (aggregator->input_finished && 
			tokens_processed == aggregator->tot_input_tokens) {
		fprintf(stderr, "Closing bucket files\n");
		for (int i=0; i<num_buckets; i++)
			fclose(fl[i]);
	}
}
