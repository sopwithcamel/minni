#include "Deserializer.h"

#define BUF_SIZE	65535

Deserializer::Deserializer(Aggregator* agg,
			const uint64_t num_buckets, 
			const char* inp_prefix, 
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p),
            size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
        max_keys_per_token(max_keys),
		num_buckets(num_buckets),
		buckets_processed(0),
		cur_bucket(NULL),
		next_buffer(0),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	size_t num_buffers = aggregator->getNumBuffers();

	inputfile_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(inputfile_prefix, inp_prefix);

	pao_list = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
	read_buf = malloc(BUF_SIZE + 1);
    for (uint32_t j=0; j<num_buffers; j++) {
        for (uint64_t i=0; i<max_keys_per_token; i++) {
            createPAO(NULL, &((*pao_list)[j][i]));
        }
    }
}

Deserializer::~Deserializer()
{
	size_t num_buffers = aggregator->getNumBuffers();
	free(inputfile_prefix);
    for (uint32_t j=0; j<num_buffers; j++) {
        for (uint64_t i=0; i<max_keys_per_token; i++) {
            destroyPAO((*pao_list)[j][i]);
        }
    }
	delete pao_list;
	delete send;
	free(read_buf);
}


void* Deserializer::operator()(void*)
{
	if (aggregator->input_finished)
		return NULL;
	bool eof_reached = false;
	size_t pao_list_ctr = 0;
	size_t num_buffers = aggregator->getNumBuffers();
	PartialAgg* new_pao;

	PartialAgg** this_list = (*pao_list)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	this_send->flush_hash = false;
	next_buffer = (next_buffer + 1) % num_buffers;

	if (!cur_bucket) { // new bucket has to be opened
		string file_name = inputfile_prefix;
		stringstream ss;
		ss << buckets_processed++;
		file_name = file_name + ss.str();
		cur_bucket = fopen(file_name.c_str(), "rb");
		fprintf(stderr, "opening file %s\n", file_name.c_str());
	}

	try {
		while (!feof(cur_bucket)) {
			this_list[pao_list_ctr++]->deserialize(cur_bucket, read_buf,
                    BUF_SIZE);
            if (pao_list_ctr == max_keys_per_token)
                goto ship_tokens;
		}
	} catch(...) {
		fprintf(stderr, "error reading file\n");
		exit(1);
	}
		
	eof_reached = true;
ship_tokens:
	aggregator->tot_input_tokens++;

	if (eof_reached) {
		fclose(cur_bucket);
		cur_bucket = NULL;
		// ask hashtable to flush itself afterwards
		this_send->flush_hash = true;
		if (buckets_processed == num_buckets) {
			aggregator->input_finished = true;
		}
	}
//	fprintf(stderr, "list size: %d\n", pao_list_ctr);
	this_send->result = this_list;
	this_send->length = pao_list_ctr;
	return this_send;
}

