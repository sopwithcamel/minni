#include "ExternalHasher.h"
#include "ProtobufPartialAgg.h"

#define		BUF_SIZE		65535

using leveldb::NewBloomFilterPolicy;

ExternalHasher::ExternalHasher(Aggregator* agg, 
			const char* ht_name,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) :
		filter(/*serial=*/true),
		aggregator(agg),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc),
		max_keys_per_token(max_keys),
        num_evicted(0),
        tokens_processed(0)
{
	options.create_if_missing = true;
	options.block_cache = leveldb::NewLRUCache(100 * 1048576);
    options.filter_policy = NewBloomFilterPolicy(10);
//	options.write_buffer_size = ;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, &db);
	assert(s.ok());
    PartialAgg* dummy;
    createPAO(NULL, &dummy);
    serializationMethod_ = dummy->getSerializationMethod();
    destroyPAO(dummy);

	uint64_t num_buffers = aggregator->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
}

ExternalHasher::~ExternalHasher()
{
	delete db;
    delete options.filter_policy;

	delete evicted_list_;
	delete send_;
}

void* ExternalHasher::operator()(void* recv)
{
	char *key;
	string value;
	size_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;
    tokens_processed++;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();
    size_t evict_list_ctr = 0;

//	fprintf(stderr, "Got %ld elements to exthash\n", recv_length);

	// Insert PAOs
    PartialAgg* p;
    createPAO(NULL, &p);
	while (ind < recv_length) {
		pao = pao_l[ind];

		leveldb::Status s = db->Get(leveldb::ReadOptions(), pao->key(),
				&value);
		if (s.ok()) {
			((ProtobufPartialAgg*)p)->deserialize(value);
			pao->merge(p);
		}
		((ProtobufPartialAgg*)pao)->serialize(&value);
		s = db->Put(leveldb::WriteOptions(), pao->key(), value);
		assert(s.ok());
		destroyPAO(pao);
		ind++;
	}
    destroyPAO(p);

	if (aggregator->can_exit) { // if everyone else has voted to exit
        if (num_evicted == 0) {
            evict_it = db->NewIterator(leveldb::ReadOptions());
            evict_it->SeekToFirst();
        }
        for (; evict_it->Valid(); evict_it->Next()) {
            createPAO(NULL, &pao);
            ((ProtobufPartialAgg*)pao)->deserialize(evict_it->value().ToString());
            this_list[evict_list_ctr++] = pao;
            if (evict_list_ctr == max_keys_per_token)
                break;
        }
        num_evicted += evict_list_ctr;
        if (evict_list_ctr < max_keys_per_token) {
            aggregator->can_exit &= true;
            num_evicted = 0;
        } else {
            aggregator->can_exit &= false;
        }
	} else {
        aggregator->can_exit &= false;
    }

    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    this_send->destroy_pao = true;
    return this_send;
}

