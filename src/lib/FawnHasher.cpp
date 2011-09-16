#include "ExternalHasher.h"
#include "Util.h"
#include <libconfig.h++>

#define BUF_SIZE	65535

ExternalHasher::ExternalHasher(Aggregator* agg, 
			const char* ht_name,
			uint64_t ext_ht_size,
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		tokens_processed(0)
{
	evictHash = FawnDS<FawnDS_Flash>::Create_FawnDS(ht_name, ext_ht_size, 0.9, 0.8, TEXT_KEYS);
}

ExternalHasher::~ExternalHasher()
{
}

void* ExternalHasher::operator()(void* pao_list)
{
	char *key, *value;
	size_t ind = 0;
	size_t last_flush_ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;

	// Insert PAOs into FawnDS 
	while (ind < recv_length) {
		pao = pao_l[ind];

		// Insert() checks for overflow
		assert(evictHash->Insert(pao->key, strlen(pao->key), pao->value, VALUE_SIZE));
		if (evictHash->checkWriteBufFlush()) {
                	for (int i=last_flush_ind+1; i<=ind; i++) {
				destroyPAO(pao_l[i]);
                	}                                            
			last_flush_ind = ind;
                }
		ind++;
	}

	/* FawnDS might have the remaining PAOs buffered, so ask it to
	 * flush explicitly and free up the PAOs */
	if (!evictHash->FlushBuffer()) {
		fprintf(stderr, "Error flushing buffer\n");
		return false;
	}
	for (int i=last_flush_ind+1; i<=ind; i++) {
		destroyPAO(pao_l[i]);
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
	ext_hash = FawnDS<FawnDS_Flash>::Open_FawnDS(ht_name);

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
}

ExthashReader::~ExthashReader()
{
	free(fl);
}

void* ExthashReader::operator()(void*)
{
	PartialAgg* pao;
	int buc;
	char k[DBID_LENGTH];
	uint32_t key_length;
	bool valid, remove;
	string val;
	uint64_t n_part = aggregator->getNumPartitions();

	char* buf = (char*)malloc(BUF_SIZE);
	
	ext_hash->split_init("");
	DBID se("a");
	while(ext_hash->split_next(&se, &se, k, key_length, val, valid, remove)) {
		if (!valid)
			continue;
		string key(k, key_length);

		int sum = 0;
		for (int i=0; i<strlen(key.c_str()); i++)
			sum += key.c_str()[i];
		buc = sum % n_part;
		if (buc < 0)
			buc += n_part;
		strcpy(buf, key.c_str());
		strcat(buf, " ");
		strcat(buf, val.c_str());
		strcat(buf, "\n");
		if (NULL == fl[buc]) {
			for (int i=0; i<strlen(pao->key); i++)
				fprintf(stderr, "%c", pao->key[i]);
			fprintf(stderr, "How possible? %d, %d, %s\n", buc, sum, pao->key);
		}
		fwrite(buf, sizeof(char), strlen(buf), fl[buc]);
	}

	fprintf(stderr, "Closing files\n");
	for (int i=0; i<n_part; i++)
		fclose(fl[i]);
	free(buf);
}
