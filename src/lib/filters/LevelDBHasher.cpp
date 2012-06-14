#include "ExternalHasher.h"
#include "ProtobufPartialAgg.h"

#define		BUF_SIZE		65535

ExternalHasher::ExternalHasher(Aggregator* agg, 
			const char* ht_name,
			size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) :
		filter(/*serial=*/true),
		aggregator(agg),
		createPAO(createPAOFunc),
		destroyPAO(destroyPAOFunc),
		max_keys_per_token(max_keys)
{
	leveldb::Options options;
	options.create_if_missing = true;
	options.block_cache = leveldb::NewLRUCache(100 * 1048576);
//	options.write_buffer_size = ;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, &db);
	assert(s.ok());
    PartialAgg* dummy;
    createPAO(NULL, &dummy);
    serializationMethod_ = dummy->getSerializationMethod();
    destroyPAO(dummy);
}

ExternalHasher::~ExternalHasher()
{
	delete db;
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

	fprintf(stderr, "Got %ld elements to exthash\n", recv_length);

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
}

