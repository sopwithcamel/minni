#include "Deserializer.h"

#define BUF_SIZE	65535

Deserializer::Deserializer(Aggregator* agg,
			const uint64_t num_buckets, 
			const char* inp_prefix, 
			PartialAgg* emptyPAO,
			PartialAgg* (*createPAOFunc)(const char* k),
			void (*destroyPAOFunc)(PartialAgg* p)) :
		aggregator(agg),
		filter(serial_in_order),
		num_buckets(num_buckets),
		buckets_processed(0),
		pao_list_ctr(0),
		pao_list_size(0),
		emptyPAO(emptyPAO),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	inputfile_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(inputfile_prefix, inp_prefix);
}

Deserializer::~Deserializer()
{
	free(inputfile_prefix);
}

uint64_t Deserializer::appendToList(PartialAgg* p)
{
	if (pao_list_ctr >= pao_list_size) {
		pao_list_size += LIST_SIZE_INCR;
		if (call_realloc(&pao_list, pao_list_size) == NULL) {
			perror("realloc failed");
			return -1;
		}
		assert(pao_list_ctr < pao_list_size);
	}
	pao_list[pao_list_ctr++] = p;
	return 1;
}


void* Deserializer::operator()(void*)
{
	FILE* inp_file;

	if (buckets_processed == num_buckets)
		return NULL;

	char* file_name = (char*)malloc(FILENAME_LENGTH);
	char* bnum = (char*)malloc(10);
	char* buf = (char*)malloc(BUF_SIZE + 1);
	char* spl;

	// To handle border cases in buffering
	PartialAgg* limbo_pao = NULL;
	char* limbo_key = NULL;
	char* limbo_val = NULL;
	bool val_set = false;

	pao_list_ctr = 0;
	pao_list_size = 0;

	// Add one element to the list so realloc doesn't complain.
	pao_list = (PartialAgg**)malloc(sizeof(PartialAgg*));
	pao_list_size++;
	
	sprintf(bnum, "%llu", buckets_processed++);
	strcpy(file_name, inputfile_prefix);
	strcat(file_name, bnum);
	inp_file = fopen(file_name, "rb");
	fprintf(stderr, "opening file %s\n", file_name);

	size_t ret;
	while (!feof(inp_file) && !ferror(inp_file)) {
		ret = fread(buf, 1, /*BUF_SIZE*/64, inp_file);
//		fprintf(stderr, "buf[0]: %c", buf[0]); 

		if (buf[0] == '\n' && limbo_val) {
			free(limbo_val);
			limbo_val = NULL;
			appendToList(limbo_pao);
			limbo_pao = NULL;
			val_set = false;
		} else if (buf[0] == ' ' && limbo_key) {
			free(limbo_key);
			limbo_key = NULL;
		}

		buf[ret] = '\0'; // fread doesn't null-terminate!
		spl = strtok(buf, " \n\r");
		if (spl == NULL) { 
			perror("Not good!");
			return NULL;
		}
		while (1) {
			if (!limbo_pao) { // starting on a fresh new PAO
				limbo_pao = createPAO(spl);
			} else { // there is something left from the last iteration
				if (limbo_val) { // val was partly read; key fully
					strcat(limbo_val, spl);
					limbo_pao->set_val(limbo_val);
					free(limbo_val);
					limbo_val = NULL;
					val_set = true;
				} else if (limbo_key) { // key was partly read
					destroyPAO(limbo_pao);
					strcat(limbo_key, spl);
					limbo_pao = createPAO(limbo_key);
					free(limbo_key);
					limbo_key = NULL;
				}
				else { // entire key was read, but not the value
					limbo_pao->set_val(spl);
					val_set = true;
				}
			}
			spl = strtok(NULL, " \n\r");
			if (spl == NULL) {
				if (!isspace(buf[ret - 1]) && buf[ret - 1] != '\n' && buf[ret-1] != '\0') {
//					fprintf(stderr, "buf[ret-1]: %c, %d", buf[ret-1], buf[ret-1]); 
					if (val_set) {
						limbo_val = (char*)malloc(VALUE_SIZE);
						strcpy(limbo_val, limbo_pao->value);
					} else {
						limbo_key = (char*)malloc(100); //TODO: Max key size!
						strcpy(limbo_key, limbo_pao->key);
					}
				}
				if (buf[ret - 1] == '\n' || buf[ret-1] == '\0') {
					if (val_set) {
						appendToList(limbo_pao);
						val_set = false;
						limbo_pao = NULL;
					}
				}
				break;
			}
			if (val_set) {
				// Add new_pao to list
				appendToList(limbo_pao);
				val_set = false;
				limbo_pao = NULL;
			}
		}
	}
	// if there was one remaining PAO in limbo, put it in.
	if (limbo_pao) {
		appendToList(limbo_pao);
		limbo_pao = NULL;
	}
	// Add emptyPAO to the list
	appendToList(emptyPAO);
	
	free(buf);
	free(bnum);
	free(file_name);

	fclose(inp_file);
//	fprintf(stderr, "list size: %d\n", pao_list_ctr);
	return pao_list;
}

