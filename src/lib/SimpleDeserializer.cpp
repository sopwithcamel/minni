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
	pao_list[pao_list_ctr++] = p;
	return pao_list_ctr;
}


void* Deserializer::operator()(void*)
{
	if (aggregator->input_finished)
		return NULL;
	char* buf = (char*)malloc(BUF_SIZE + 1);
	char* spl;
	bool eof_reached = false;
	uint64_t list_size = aggregator->getPAOsPerToken();

	pao_list_ctr = 0;
	pao_list_size = 0;
	PartialAgg* new_pao;

	pao_list = (PartialAgg**)malloc(sizeof(PartialAgg*) * list_size);

	if (!cur_bucket) { // new bucket has to be opened
		string file_name = inputfile_prefix;
		stringstream ss;
		ss << buckets_processed++;
		file_name = file_name + ss.str();
		cur_bucket = fopen(file_name.c_str(), "rb");
		fprintf(stderr, "opening file %s\n", file_name.c_str());
	}

	while (!feof(cur_bucket) && !ferror(cur_bucket)) {
		if (fgets(buf, BUF_SIZE, cur_bucket) == NULL)
			break;
		spl = strtok(buf, " \n\r");
		if (spl == NULL)
			continue;
		new_pao = createPAO(spl);
		spl = strtok(NULL, " \n\r");
		if (spl == NULL)
			continue;
		new_pao->set_val(spl);
		if (appendToList(new_pao) == list_size - 1)
			goto ship_tokens;
	}
	eof_reached = true;
ship_tokens:
	free(buf);
	aggregator->tot_input_tokens++;
	appendToList(emptyPAO);

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

