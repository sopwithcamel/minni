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
}

Deserializer::~Deserializer()
{
	size_t num_buffers = aggregator->getNumBuffers();
	free(inputfile_prefix);
	delete pao_list;
	delete send;
	free(read_buf);
}


void* Deserializer::operator()(void*)
{
	size_t pao_list_ctr = 0;
	size_t num_buffers = aggregator->getNumBuffers();
	PartialAgg* new_pao;

	PartialAgg** this_list = (*pao_list)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	this_send->flush_hash = false;
	next_buffer = (next_buffer + 1) % num_buffers;
	aggregator->tot_input_tokens++;

    if (!aggregator->sendNextToken) {
        aggregator->sendNextToken = true;
        this_send->result = NULL;
        this_send->length = 0;
        this_send->flush_hash = true;
        this_send->destroy_pao = false;
        return this_send;
    }

	if (aggregator->input_finished)
        return NULL;

	if (!cur_bucket) { // new bucket has to be opened
		string file_name = inputfile_prefix;
		stringstream ss;
		ss << buckets_processed++;
		file_name = file_name + ss.str();
		cur_bucket = fopen(file_name.c_str(), "rb");
		fprintf(stderr, "opening file %s\n", file_name.c_str());
	}

    while (!feof(cur_bucket)) {
        createPAO(NULL, &(this_list[pao_list_ctr]));
        this_list[pao_list_ctr]->deserialize(cur_bucket, read_buf,
                BUF_SIZE);
        pao_list_ctr++;
        if (pao_list_ctr == max_keys_per_token - 1) {
            break;
        }
    }

	if (feof(cur_bucket)) {
		fclose(cur_bucket);
		cur_bucket = NULL;
		// ask hashtable to flush itself afterwards
		this_send->flush_hash = true;
		if (buckets_processed == num_buckets) {
			aggregator->input_finished = true;
            aggregator->sendNextToken = true;
		}
	} else
        this_send->flush_hash = false;
//	fprintf(stderr, "list size: %d\n", pao_list_ctr);
	this_send->result = this_list;
	this_send->length = pao_list_ctr;
    this_send->destroy_pao = true;
	return this_send;
}

