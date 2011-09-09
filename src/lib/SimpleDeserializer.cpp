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
	PartialAgg* new_pao;

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
	}

	while (!feof(cur_bucket) && !ferror(cur_bucket)) {
		if (pao_list_ctr > PAOS_IN_TOKEN) {
			goto ship_tokens;
		}

		buf = fgets(buf, BUF_SIZE, cur_bucket);
		if (buf == NULL)
			break;
		spl = strtok(buf, " \n\r");
		new_pao = createPAO(spl);
		spl = strtok(NULL, " \n\r");
		new_pao->set_val(spl);
		appendToList(new_pao);
	}
	eof_reached = true;
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

