#include "Serializer.h"

#define BUF_SIZE	65535

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
	}
	free(fname);
	buf = (char*)malloc(BUF_SIZE + 1);
	type = aggregator->getType();
}

Serializer::~Serializer()
{
	free(buf);
	free(fl);
}

void Serializer::setInputAlreadyPartitioned()
{
	already_partitioned = true;
}

int Serializer::partition(const char* key)
{
	int buc, sum = 0;
	for (int i=type; i<strlen(key); i+=2)
		sum += key[i];
	buc = sum % num_buckets;
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

	if (already_partitioned) {
		/* apply partition function to one key to see which
		 * file to write to. We know that all the other PAOs
		 *  in the list will go to the same file. */
		if (recv_length >= 1)
			buc = partition(pao_l[0]->key);
	}

	while(ind < recv_length) {
		pao = pao_l[ind];
		if (!already_partitioned)
			buc = partition(pao->key);	
		strcpy(buf, pao->key);
		strcat(buf, " ");
		strcat(buf, pao->value);
		strcat(buf, "\n");
		if (NULL == fl[buc]) {
			for (int i=0; i<strlen(pao->key); i++)
				fprintf(stderr, "%c", pao->key[i]);
			fprintf(stderr, "How possible? %d, %s\n", buc, pao->key);
		}
		fwrite(buf, sizeof(char), strlen(buf), fl[buc]);
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
