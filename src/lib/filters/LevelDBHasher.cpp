#include "ExternalHasher.h"

#define		BUF_SIZE		65535

ExternalHasher::ExternalHasher(Aggregator* agg, 
			leveldb::DB** db_ptr,
			const char* ht_name,
			uint64_t ext_ht_size,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) :
		filter(/*serial=*/true),
		aggregator(agg),
		max_keys_per_token(max_keys),
		db(db_ptr),
		destroyPAO(destroyPAOFunc)
{
	leveldb::Options options;
	options.create_if_missing = true;
//	options.block_cache = cache_;
//	options.write_buffer_size = ;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, db);
	assert(s.ok());
	buf = (char*)malloc(BUF_SIZE);
}

ExternalHasher::~ExternalHasher()
{
	delete *db;
	free(buf);
}

void* ExternalHasher::operator()(void* recv)
{
	char *key;
	char* buf = (char*)malloc(BUF_SIZE);
	string value;
	size_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;

	fprintf(stderr, "Got %ld elements to exthash\n", recv_length);

	// Insert PAOs
	while (ind < recv_length) {
		pao = pao_l[ind];

		leveldb::Status s = (*db)->Get(leveldb::ReadOptions(), pao->key,
				&value);
		if (s.ok()) {
			strcpy(buf, value.c_str());
			pao->add((void*)buf);
		}
		pao->serialize((void*)buf);
		fprintf(stderr, "Writing %s to ext\n", buf);
		s = (*db)->Put(leveldb::WriteOptions(), pao->key, buf);
		assert(s.ok());
		destroyPAO(pao);
		ind++;
	}
	free(buf);
}

ExternalHashReader::ExternalHashReader(Aggregator* agg, 
			leveldb::DB** db_ptr,
			const char* ht_name,
			uint64_t ext_ht_size,
			PartialAgg* (*createPAOFunc)(char** t, size_t* ts),
			const size_t max_keys) :
		filter(/*serial=*/true),
		aggregator(agg),
		max_keys_per_token(max_keys),
		db(db_ptr),
		createPAO(createPAOFunc)
{
	leveldb::Options options;
	options.create_if_missing = true;
//	options.block_cache = cache_;
//	options.write_buffer_size = ;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, db);
	assert(s.ok());

	uint64_t num_buffers = aggregator->getNumBuffers();
	pao_list = new MultiBuffer<PartialAgg*>(num_buffers, max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

ExternalHashReader::~ExternalHashReader()
{
	delete *db;
	delete pao_list;
	delete send;
}

void* ExternalHashReader::operator()(void* recv)
{
	char *key;
	string value;
	size_t ind = 0;
	char* buf = (char*)malloc(BUF_SIZE);

	FilterInfo* recv_list = (FilterInfo*)recv;
	char*** tok_list = (char***)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;

	uint64_t num_buffers = aggregator->getNumBuffers();
	FilterInfo* this_send = (*send)[next_buffer];
	PartialAgg** this_pao_list = (*pao_list)[next_buffer];
	next_buffer = (next_buffer + 1) % num_buffers;

	// Insert PAOs
	while (ind < recv_length) {
		key = tok_list[ind][0];
		leveldb::Status s = (*db)->Get(leveldb::ReadOptions(), key,
				&value);
		if (s.ok()) {
			strcpy(buf, value.c_str());
			this_pao_list[ind] = createPAO(NULL, NULL);
			this_pao_list[ind]->deserialize((void*)buf);
		} else
			this_pao_list[ind] = NULL;
		ind++;
	}
	this_send->result2 = this_pao_list;
pass_through:
	this_send->result = recv_list->result;
	this_send->result1 = recv_list->result1;
	free(buf);
}

ExternalHashSerializer::ExternalHashSerializer(Aggregator* agg,
			leveldb::DB** ptr_to_db,
			const char* ht_name,
			uint64_t ext_capacity, 
			const char* outfile_prefix) :
		filter(serial_in_order),
		aggregator(agg),
		db(ptr_to_db),
		ht_name(ht_name),
		external_capacity(ext_capacity)
{
	char num[10];
	/* Set up external hashtable for scanning */
	leveldb::Options options;
	options.create_if_missing = false;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, db);
	assert(s.ok());

	/* Set up files for writing key value pairs to */
	char* fname = (char*)malloc(FILENAME_LENGTH);
	uint64_t n_part = aggregator->getNumPartitions();
	fl = (FILE**)malloc(n_part * sizeof(FILE*));
	for (int i=0; i<n_part; i++) {
		sprintf(num, "%d", i);
		strcpy(fname, outfile_prefix);
		strcat(fname, num);
		fl[i] = fopen(fname, "w");
	}
	fprintf(stderr, "Serializing to file: %s\n", fname);
	free(fname);
	buf = (char*)malloc(BUF_SIZE);
}

ExternalHashSerializer::~ExternalHashSerializer()
{
	delete *db;
	free(fl);
	free(buf);
}

void* ExternalHashSerializer::operator()(void*)
{
	PartialAgg* pao;
	int buc;
	uint32_t key_length;
	bool valid, remove;
	string val;
	uint64_t n_part = aggregator->getNumPartitions();

	leveldb::Iterator* iter = (*db)->NewIterator(leveldb::ReadOptions());
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		const char* k = iter->key().data();
		uint32_t sum = 0;
		for (int i=0; i<strlen(k); i++)
			sum += k[i];
		buc = sum % n_part;
		if (buc < 0)
			buc += n_part;
		strcpy(buf, iter->value().ToString().c_str());
		strcat(buf, "\n");
		fwrite(buf, 1, strlen(buf), fl[buc]);
	}
	fprintf(stderr, "Closing files\n");
	for (int i=0; i<n_part; i++)
		fclose(fl[i]);
	return NULL;
}
