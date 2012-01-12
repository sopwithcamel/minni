#include "TestRR.h"
#include "Util.h"
#include <util.h>

#define BUF_SIZE	65535

ExternalHasher::ExternalHasher(Aggregator* agg, 
			const char* ht_name,
			uint64_t ext_ht_size,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		destroyPAO(destroyPAOFunc),
		tokens_processed(0)
{
	int i;
	long file_size = 6442450944;
	fd = open("/mnt/hamur/randinput.txt", O_RDONLY);
        for (i = 0; i < NUM_READS; i++) {
                arr[i] = rand() % file_size;
        }

}

ExternalHasher::~ExternalHasher()
{
	close(fd);
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
        char* buf = (char*)malloc(4096);

	// Insert PAOs into FawnDS 
	while (ind < recv_length) {
		pao = pao_l[ind];
		if (pread64(fd, buf, 4096, arr[ind]) != 4096) {
                        perror("Could not read");
                }
		destroyPAO(pao);
		ind++;
	}

}
