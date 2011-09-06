#include "Deserializer.h"

#define BUF_SIZE	65535
#define PAOS_IN_TOKEN	20000

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
		limbo_pao(NULL),
		limbo_key(NULL),
		limbo_val(NULL),
		val_set(false),
		cur_bucket(NULL),
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
	if (aggregator->input_finished)
		return NULL;
	char* buf = (char*)malloc(BUF_SIZE + 1);
	char* spl;
	bool eof_reached = false;

	pao_list_ctr = 0;
	pao_list_size = 0;

	// Add one element to the list so realloc doesn't complain.
	pao_list = (PartialAgg**)malloc(sizeof(PartialAgg*));
	pao_list_size++;

	if (!cur_bucket) { // new bucket has to be opened
		string file_name = inputfile_prefix;
		stringstream ss;
		ss << buckets_processed++;
		file_name = file_name + ss.str();
		cur_bucket = fopen(file_name.c_str(), "rb");
		fprintf(stderr, "opening file %s\n", file_name.c_str());

		// To handle border cases in buffering
		limbo_pao = NULL;
		limbo_key = NULL;
		limbo_val = NULL;
		val_set = false;
	}

	size_t ret;
	while (!feof(cur_bucket) && !ferror(cur_bucket)) {
		if (pao_list_ctr > PAOS_IN_TOKEN) {
			goto ship_tokens;
		}

		ret = fread(buf, 1, BUF_SIZE, cur_bucket);

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
			exit(1);
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

	// this point reached because EOF is reached for bucket
	eof_reached = true;

	// if there was one remaining PAO in limbo, put it in.
	if (limbo_pao) {
		appendToList(limbo_pao);
		limbo_pao = NULL;
	}

ship_tokens:
	// Add emptyPAO to the list
	appendToList(emptyPAO);

	free(buf);
	aggregator->tot_input_tokens++;
	
	if (eof_reached) {
		fclose(cur_bucket);
		cur_bucket = NULL;
		if (buckets_processed == num_buckets) {
			aggregator->input_finished = true;
		}
	}
//	fprintf(stderr, "list size: %d\n", pao_list_ctr);
	return pao_list;
}

