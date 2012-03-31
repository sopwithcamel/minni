#include "Serializer.h"

using namespace google::protobuf::io;

#define BUF_SIZE	65535

Serializer::Serializer(Aggregator* agg,
			const uint64_t nb, 
			const char* outfile_prefix,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(serial_in_order),
		aggregator(agg),
		already_partitioned(false),
		num_buckets(nb),
		tokens_processed(0),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc)
{
	char num[10];
	char* fname = (char*)malloc(FILENAME_LENGTH);

    PartialAgg* dummy;
    createPAO(NULL, &dummy);
    serializationMethod_ = dummy->getSerializationMethod();
    destroyPAO(dummy);

	for (int i=0; i<num_buckets; i++) {
		sprintf(num, "%d", i);
		strcpy(fname, outfile_prefix);
		strcat(fname, num);

        std::ofstream* of = new std::ofstream(fname, ios::out|ios::binary);
        fl_.push_back(of);
        switch (serializationMethod_) {
            case PartialAgg::PROTOBUF:
                raw_output_.push_back(new OstreamOutputStream(fl_[i]));
                coded_output_.push_back(new CodedOutputStream(raw_output_[i]));
                break;
            case PartialAgg::BOOST:
                oa_.push_back(new boost::archive::binary_oarchive(*fl_[i]));
                break;
        }
		assert(NULL != fl_[i]);
	}
	free(fname);
	type = aggregator->getType();
}

Serializer::~Serializer()
{
}

int Serializer::partition(const std::string& key)
{
	int buc, sum = 0;
	for (int i=0; i<key.size(); i++)
		sum += key[i];
	buc = (sum + type) % num_buckets;
	if (buc < 0)
		buc += num_buckets;
	return buc;
}

void* Serializer::operator()(void* pao_list)
{
	PartialAgg* pao;
	int buc;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;
    tokens_processed++;

	uint64_t ind = 0;
    while(ind < recv_length) {
        pao = pao_l[ind];
        buc = partition(pao->key());	
        assert(pao != NULL);
        switch (serializationMethod_) {
            case PartialAgg::PROTOBUF:
                ((ProtobufPartialAgg*)pao)->serialize(coded_output_[buc]);
                break;
            case PartialAgg::BOOST:
                ((BoostPartialAgg*)pao)->serialize(oa_[buc]);
                break;
            case PartialAgg::HAND:
                ((HandSerializedPartialAgg*)pao)->serialize(fl_[buc]);
                break;
        }
        if (recv->destroy_pao)
            destroyPAO(pao);
        ind++;
    }

    if (aggregator->input_finished && 
            tokens_processed == aggregator->tot_input_tokens &&
            aggregator->sendNextToken == true) {

        for (int i=0; i<num_buckets; i++) {
            fl_[i]->close();
            switch(serializationMethod_) {
                case PartialAgg::PROTOBUF:
                    delete coded_output_[i];
                    delete raw_output_[i];
                    break;
                case PartialAgg::BOOST:
                    delete oa_[i];
                    break;
            }
            delete fl_[i];
        }

    }
}
