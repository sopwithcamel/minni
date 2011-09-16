#include "ExternalHasher.h"
#include "Util.h"
#include <libconfig.h++>
#include "leveldb/db.h"

#define		BUF_SIZE		65535

ExternalHasher::ExternalHasher(Aggregator* agg, 
			const char* ht_name,
			uint64_t ext_ht_size,
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		db(NULL),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc)
{
	leveldb::Options options;
	options.create_if_missing = true;
//	options.block_cache = cache_;
//	options.write_buffer_size = ;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, &db);
	assert(s.ok());
}

ExternalHasher::~ExternalHasher()
{
	delete db;
}

void* ExternalHasher::operator()(void* pao_list)
{
	char *key;
	string value;
	size_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;

	fprintf(stderr, "Got %d elements to exthash\n", recv_length);

	// Insert PAOs into FawnDS 
	while (ind < recv_length) {
		pao = pao_l[ind];

		leveldb::Status s = db->Get(leveldb::ReadOptions(), pao->key,
				&value);
		if (s.ok()) {
			pao->add(value.c_str());
		}
		s = db->Put(leveldb::WriteOptions(), pao->key, pao->value);
		assert(s.ok());
		destroyPAO(pao);
		ind++;
	}
}

ExthashReader::ExthashReader(Aggregator* agg,
			const char* ht_name,
			uint64_t ext_capacity, 
			PartialAgg* emptyPAO,
			const char* outfile_prefix) :
		filter(serial_in_order),
		aggregator(agg),
		emptyPAO(emptyPAO),
		ht_name(ht_name),
		external_capacity(ext_capacity)
{
	char num[10];
	/* Set up external hashtable for scanning */
	leveldb::Options options;
	options.create_if_missing = false;
	leveldb::Status s = leveldb::DB::Open(options, ht_name, &db);
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
	free(fname);
	buf = (char*)malloc(BUF_SIZE);
}

ExthashReader::~ExthashReader()
{
	delete db;
	free(fl);
	free(buf);
}

void* ExthashReader::operator()(void*)
{
	PartialAgg* pao;
	int buc;
	uint32_t key_length;
	bool valid, remove;
	string val;
	uint64_t n_part = aggregator->getNumPartitions();

	leveldb::Iterator* iter = db->NewIterator(leveldb::ReadOptions());
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		const char* k = iter->key().data();
		uint32_t sum = 0;
		for (int i=0; i<strlen(k); i++)
			sum += k[i];
		buc = sum % n_part;
		if (buc < 0)
			buc += n_part;
		strcpy(buf, k);
		strcat(buf, " ");
		strcat(buf, iter->value().data());
		strcat(buf, "\n");
	}
	fprintf(stderr, "Closing files\n");
	for (int i=0; i<n_part; i++)
		fclose(fl[i]);
	return NULL;
}
