#include "Serializer.h"

using namespace google::protobuf::io;

#define BUF_SIZE	65535

Serializer::Serializer(Aggregator* agg,
			const uint64_t nb, 
			const char* o_prefix) :
		filter(serial_in_order),
		aggregator(agg),
		already_partitioned(false),
		num_buckets(nb),
		tokens_processed(0)
{
	type = aggregator->getType();
    outfile_prefix = (char*)malloc(FILENAME_LENGTH);
    strcpy(outfile_prefix, o_prefix);
}

Serializer::~Serializer()
{
    free(outfile_prefix);
}

int Serializer::partition(const char* key) const
{
	int buc, sum = 0;
	for (int i=0; i<strlen(key); i++)
		sum += key[i];
	buc = (sum + type) % num_buckets;
	if (buc < 0)
		buc += num_buckets;
	return buc;
}

void Serializer::reset()
{
    tokens_processed = 0;
}

void* Serializer::operator()(void* pao_list)
{
	PartialAgg* pao;
	int buc;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;

    if (tokens_processed == 0) {
        char num[10];
        char* fname = (char*)malloc(FILENAME_LENGTH);
        serializationMethod_ = aggregator->ops()->getSerializationMethod();

        for (int i=0; i<num_buckets; i++) {
            sprintf(num, "%d", i);
            strcpy(fname, outfile_prefix);
            strcat(fname, num);

            std::ofstream* of = new std::ofstream(fname, ios::out|ios::binary);
            fl_.push_back(of);
            switch (serializationMethod_) {
                case Operations::PROTOBUF:
                    raw_output_.push_back(new OstreamOutputStream(fl_[i]));
                    coded_output_.push_back(new CodedOutputStream(raw_output_[i]));
                    break;
                case Operations::BOOST:
                    oa_.push_back(new boost::archive::binary_oarchive(*fl_[i]));
                    break;
            }
            assert(NULL != fl_[i]);
        }
        free(fname);
    }

    tokens_processed++;

	uint64_t ind = 0;
    const Operations* const op = aggregator->ops();

    while(ind < recv_length) {
        pao = pao_l[ind];
        assert(pao != NULL);
        buc = partition(op->getKey(pao));
        switch (serializationMethod_) {
            case Operations::PROTOBUF:
                ((ProtobufOperations*)op)->serialize(pao, coded_output_[buc]);
                break;
            case Operations::BOOST:
                ((BoostOperations*)op)->serialize(pao, oa_[buc]);
                break;
        }
        if (recv->destroy_pao)
            op->destroyPAO(pao);
        ind++;
    }

    /* The first condition checks that the first stage has processed
     * all input. The second checks that all the tokens have arrived
     * at this, the last stage. The third checks that no stage in the
     * middle has any data left to send */
    if (aggregator->input_finished && 
            tokens_processed == aggregator->tot_input_tokens  &&
            aggregator->can_exit == true) {
        for (int i=0; i<num_buckets; i++) {
            switch(serializationMethod_) {
                case Operations::PROTOBUF:
                    delete coded_output_[i];
                    delete raw_output_[i];
                    break;
                case Operations::BOOST:
                    delete oa_[i];
                    break;
            }
            fl_[i]->close();
            delete fl_[i];
        }
        // reset for possible reuse
        fl_.clear();
        raw_output_.clear();
        coded_output_.clear();
        oa_.clear();
        tokens_processed = 0;
        aggregator->can_exit &= true;
    } else
        aggregator->can_exit &= false;
}
