#include "Serializer.h"

Serializer::Serializer(Aggregator* agg,
			PartialAgg* emptyPAO,
			const uint64_t nb, 
			const char* outfile_prefix,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(serial_in_order),
		aggregator(agg),
		emptyPAO(emptyPAO),
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
}

Serializer::~Serializer()
{
}

void* Serializer::operator()(void* pao_list)
{
	PartialAgg* pao;
	int ind = 0, buc;
	PartialAgg** pao_l = (PartialAgg**)pao_list;
	char* buf = (char*)malloc(VALUE_SIZE * 10);
	while(strcmp(pao_l[ind]->key, emptyPAO->key)) {
		pao = pao_l[ind];
		// TODO; use another partitioning function later!
		int sum = 0;
		for (int i=0; i<strlen(pao->key); i++)
			sum += pao->key[i];
		buc = sum % num_buckets;
		if (buc < 0)
			buc += num_buckets;
		strcpy(buf, pao->key);
		strcat(buf, " ");
		strcat(buf, pao->value);
		strcat(buf, "\n");
		if (NULL == fl[buc]) {
			for (int i=0; i<strlen(pao->key); i++)
				fprintf(stderr, "%c", pao->key[i]);
			fprintf(stderr, "How possible? %d, %d, %s\n", buc, sum, pao->key);
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
		free(fl);
	}

	free(buf);
	free(pao_l);
}
