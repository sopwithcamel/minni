#include "config.h"
#include "Serializer.h"

Serializer::Serializer(MapperAggregator* agg, const uint64_t nb, 
	const char* f_prefix, void (*destroyPAOFunc)(PartialAgg* p)) :
		aggregator(agg),
		filter(serial_in_order),
		num_buckets(nb),
		fname_prefix(f_prefix),
		destroyPAO(destroyPAOFunc)
{
	char num[10];
	char* gen_fname = (char*)malloc(FILENAME_PREFIX_LENGTH + 10);
	char* fname = (char*)malloc(FILENAME_PREFIX_LENGTH + 10);

	fl = (FILE**)malloc(num_buckets * sizeof(FILE*));

	strcpy(gen_fname, fname_prefix);
	strcat(gen_fname, "bucket");
	for (int i=0; i<num_buckets; i++) {
		sprintf(num, "%d", i);
		strcpy(fname, gen_fname);
		strcat(fname, num);

		fl[i] = fopen(fname, "w");
	}
	free(gen_fname);
	free(fname);
}

Serializer::~Serializer()
{
	for (int i=0; i<num_buckets; i++) {
		fclose(fl[i]);
	}
	free(fl);
}

void* Serializer::operator()(void* pao_list)
{
	PartialAgg* pao;
	int ind = 0, buc;
	PartialAgg** pao_l = (PartialAgg**)pao_list;
	char* buf = (char*)malloc(VALUE_SIZE * 10);
	while(strcmp(pao_l[ind]->key, EMPTY_KEY)) {
		pao = pao_l[ind];
		// TODO; use another partitioning function later!
		int sum = 0;
		for (int i=0; i<strlen(pao->key); i++)
			sum += pao->key[i];
		buc = sum % num_buckets;
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
	free(buf);
	free(pao_l);
}
