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
		cur_bucket(NULL),
		next_buffer(0),
		emptyPAO(emptyPAO),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	size_t num_buffers = aggregator->getNumBuffers();

	inputfile_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(inputfile_prefix, inp_prefix);

	pao_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers);
}

Deserializer::~Deserializer()
{
	free(inputfile_prefix);
	free(pao_list);
}


void* Deserializer::operator()(void*)
{
	if (aggregator->input_finished)
		return NULL;
	char* buf = (char*)malloc(BUF_SIZE + 1);
	char* spl;
	bool eof_reached = false;
	uint64_t list_size = aggregator->getPAOsPerToken();

	size_t pao_list_ctr = 0;
	PartialAgg* new_pao;
	size_t num_buffers = aggregator->getNumBuffers();

	pao_list[next_buffer] = (PartialAgg**)malloc(sizeof(PartialAgg*) * list_size);
	PartialAgg** this_list = pao_list[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers;

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
		this_list[pao_list_ctr++] = new_pao;
		if (pao_list_ctr == list_size - 1)
			goto ship_tokens;
	}
	eof_reached = true;
ship_tokens:
	free(buf);
	aggregator->tot_input_tokens++;
	this_list[pao_list_ctr++] = emptyPAO;

	if (eof_reached) {
		fclose(cur_bucket);
		cur_bucket = NULL;
		if (buckets_processed == num_buckets) {
			aggregator->input_finished = true;
		}
	}
//	fprintf(stderr, "list size: %d\n", pao_list_ctr);
	return this_list;
}

