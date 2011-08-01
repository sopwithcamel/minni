#include "config.h"
#include "Deserializer.h"

#define BUF_SIZE	65535

Deserializer::Deserializer(MapperAggregator* agg,
			const uint64_t num_buckets, 
			const char* inp_prefix, 
			PartialAgg* emptyPAO,
			PartialAgg* (*MapFunc)(const char* k)) :
		aggregator(agg),
		filter(serial_in_order),
		num_buckets(num_buckets),
		buckets_processed(0),
		inputfile_prefix(inp_prefix),
		emptyPAO(emptyPAO),
		Map(MapFunc)
{
}

Deserializer::~Deserializer()
{
}

void* Deserializer::operator()(void*)
{
	FILE* inp_file;
	char* file_name = (char*)malloc(FILENAME_PREFIX_LENGTH + 10);
	char* bnum = (char*)malloc(10);
	char* buf = (char*)malloc(BUF_SIZE + 1);
	char* spl;

	uint64_t pao_list_ctr = 0;
	uint64_t pao_list_size = 1;
	
	sprintf(bnum, "%llu", buckets_processed++);
	strcpy(file_name, inputfile_prefix);
	strcat(file_name, "bucket");
	strcat(file_name, bnum);
	inp_file = fopen(file_name, "rb");
	fprintf(stderr, "opening file %s\n", file_name);

	// Add one element to the list so realloc doesn't complain.
	pao_list = (PartialAgg**)malloc(sizeof(PartialAgg*));

	while (!feof(inp_file) && !ferror(inp_file)) {
		fread(buf, BUF_SIZE, 1, inp_file);
		spl = strtok(buf, " \n\r");
		if (spl == NULL) { 
			perror("Not good!");
			return NULL;
		}
		while (1) {
			PartialAgg* new_pao = Map(spl);

			spl = strtok(NULL, " \n\r");
			if (spl == NULL) {
				perror("File ended after reading key!");
				fprintf(stderr, "%s\n", new_pao->key);
				break;
			}
			new_pao->set_val(spl);

			// Add new_pao to list
			if (pao_list_ctr >= pao_list_size) {
				pao_list_size += LIST_SIZE_INCR;
				if (call_realloc(&pao_list, pao_list_size) == NULL) {
					perror("realloc failed");
					return NULL;
				}
				assert(pao_list_ctr < pao_list_size);
			}
			pao_list[pao_list_ctr++] = new_pao;

			// Read in next key
			spl = strtok(NULL, " \n\r");
			if (spl == NULL) {
				perror("File ended after reading key!");
				break;
			}
		}
	}

	// Add emptyPAO to the list
	if (pao_list_ctr >= pao_list_size) {
		pao_list_size += LIST_SIZE_INCR;
		if (call_realloc(&pao_list, pao_list_size) == NULL) {
			perror("realloc failed");
			return NULL;
		}
		assert(pao_list_ctr < pao_list_size);
	}
	pao_list[pao_list_ctr++] = emptyPAO;
	
	free(buf);
	free(bnum);
	free(file_name);

	fclose(inp_file);
	return pao_list;
}

