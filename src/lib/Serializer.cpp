#include "config.h"
#include "Serializer.h"

Serializer::Serializer(const uint64_t nb, const char* f_prefix) :
		filter(serial_in_order),
		num_buckets(nb),
		fname_prefix(f_prefix)
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
		buc = pao->key[0] % num_buckets;
		strcpy(buf, pao->key);
		strcat(buf, " ");
		strcat(buf, pao->value);
		strcat(buf, " ");
		fwrite(buf, sizeof(char), strlen(buf), fl[buc]);
		ind++;
	}
	free(buf);
	free(pao_l);
}
