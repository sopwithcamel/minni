#include "config.h"
#include "ExternalHasher.h"
#include "Util.h"

ExternalHasher::ExternalHasher(Aggregator* agg, 
			char* ht_name,
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		tokens_processed(0)
{
	evictHash = FawnDS<FawnDS_Flash>::Create_FawnDS(ht_name, EXT_HASH_SIZE, 0.9, 0.8, TEXT_KEYS);
}

ExternalHasher::~ExternalHasher()
{
}

void* ExternalHasher::operator()(void* pao_list)
{
	char *key, *value;
	PartialAgg** pao_l = (PartialAgg**)pao_list;
	size_t ind = 0;
	size_t last_flush_ind = 0;
	PartialAgg* pao;

	// Insert PAOs into FawnDS 
	while (strcmp((pao_l[ind])->key, emptyPAO->key)) { 
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

	// free PAO list
	free(pao_l);
}

