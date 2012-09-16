#include "config.h"
#include "Deserializer.h"

using namespace google::protobuf::io;

#define BUF_SIZE	65535

Deserializer::Deserializer(Aggregator* agg,
			const uint64_t num_buckets, 
			const char* inp_prefix, 
            size_t max_keys) :
		aggregator(agg),
		filter(serial_in_order),
        max_keys_per_token(max_keys),
		num_buckets(num_buckets),
		buckets_processed(0),
		cur_bucket(NULL),
		next_buffer(0)
{
	size_t num_buffers = aggregator->getNumBuffers();

	inputfile_prefix = (char*)malloc(FILENAME_LENGTH);
	strcpy(inputfile_prefix, inp_prefix);

	pao_list = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
	read_buf = malloc(BUF_SIZE + 1);

    serializationMethod_ = aggregator->ops()->getSerializationMethod();
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

    if (aggregator->input_finished) {
        if (aggregator->can_exit)
            return NULL;
        else {
            aggregator->can_exit = true;
            aggregator->stall_pipeline = false;

            this_send->result = NULL;
            this_send->length = 0;
            this_send->flush_hash = true;
            this_send->destroy_pao = false;
            // still have to count this as a token
            aggregator->tot_input_tokens++;
            return this_send;
        }
    }
    if (aggregator->stall_pipeline) {
        aggregator->stall_pipeline = false;

        this_send->result = NULL;
        this_send->length = 0;
        this_send->flush_hash = true;
        this_send->destroy_pao = false;
        // still have to count this as a token
        aggregator->tot_input_tokens++;
        return this_send;
    } else {
        aggregator->can_exit = false;
        aggregator->stall_pipeline = false;
    }
	aggregator->tot_input_tokens++;

	if (!cur_bucket) { // new bucket has to be opened
		string file_name = inputfile_prefix;
		stringstream ss;
		ss << buckets_processed++;
		file_name = file_name + ss.str();
		cur_bucket = new std::ifstream(file_name.c_str(), ios::in|ios::binary);
        switch (serializationMethod_) {
            case Operations::PROTOBUF:
                raw_input = new IstreamInputStream(cur_bucket);
                break;
            case Operations::BOOST:
                ia_ = new boost::archive::binary_iarchive(*cur_bucket);
                break;
        }
        assert(cur_bucket->is_open());
		fprintf(stderr, "opening file %s\n", file_name.c_str());
	}

    switch (serializationMethod_) {
        case Operations::PROTOBUF:
            coded_input = new CodedInputStream(raw_input);
            coded_input->SetTotalBytesLimit(1073741824, -1);
            break;
    }

    bool eof_reached = false;
    const Operations* const op = aggregator->ops();

    while (true) {
        op->createPAO(NULL, &(this_list[pao_list_ctr]));
        bool ret;
        switch (serializationMethod_) {
            case Operations::PROTOBUF:
                {
                    ret = ((ProtobufOperations*)op)->deserialize(
                            this_list[pao_list_ctr], coded_input);
                    break;
                }
            case Operations::BOOST:
                {
                    ret = ((BoostOperations*)op)->deserialize(
                            this_list[pao_list_ctr], ia_);
                    break;
                }
/*
            case PartialAgg::HAND:
                ret = ((HandSerializedPartialAgg*)this_list[
                        pao_list_ctr])->deserialize(cur_bucket);
                break;
*/
        }
        if (!ret) {
            op->destroyPAO(this_list[pao_list_ctr]);
            this_list[pao_list_ctr] = NULL;
            eof_reached = true;
            break;           
        }
        pao_list_ctr++;
        if (pao_list_ctr == max_keys_per_token - 1) {
            break;
        }
    }

    switch (serializationMethod_) {
        case Operations::PROTOBUF:
            delete coded_input;
            break;
    }

	if (eof_reached) {
        switch (serializationMethod_) {
            case Operations::PROTOBUF:
                delete coded_input;
                delete raw_input;
                break;
            case Operations::BOOST:
                delete ia_;
                break;
        }
		cur_bucket->close();
        delete cur_bucket;
		cur_bucket = NULL;
		// ask hashtable to flush itself afterwards
		this_send->flush_hash = true;
		if (buckets_processed == num_buckets) {
			aggregator->input_finished = true;
            aggregator->can_exit = true;
            buckets_processed = 0;
		}
	} else
        this_send->flush_hash = false;
//	fprintf(stderr, "list size: %d\n", pao_list_ctr);
	this_send->result = this_list;
	this_send->length = pao_list_ctr;
    this_send->destroy_pao = true;
	return this_send;
}

