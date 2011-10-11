#include "Store.h"

Store::Store(Aggregator* agg, 
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) :
		aggregator(agg),
		destroyPAO(destroyPAOFunc),
		max_keys_per_token(max_keys),
		store_size(10000000),		// TODO set from config
		next_buffer(0),
		tokens_processed(0)
{
	hasher = new StoreHasher(this);
	agger = new StoreAggregator(this);
	writer = new StoreWriter(this);

	//TODO: read name from config
	store_fd = open("/mnt/hamur/store.ht", O_CREAT|O_RDWR|O_TRUNC, S_IRUSR|S_IWUSR);
	assert(store_fd > 0);

	occup = (uint64_t*)calloc(store_size, sizeof(uint64_t));
	assert(occup != NULL);
}

Store::~Store()
{
	delete hasher;
	delete agger;
	delete writer;
	close(store_fd);
	free(occup);
}

bool Store::slot_occupied(size_t bin_index, size_t slot_index)
{
	uint64_t mask = 0x1 << slot_index;
	if ((occup[bin_index] & mask) > 0)
		return true;
	return false;
}

void Store::set_slot_occupied(size_t bin_offset, size_t bin_index)
{
	uint64_t mask = 1 << bin_index;
	occup[bin_offset] |= mask;
}


StoreHasher::StoreHasher(Store* store) :
		filter(/*serial=*/true),
		store(store),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = store->aggregator->getNumBuffers();
	offset_list = (uint64_t**)malloc(sizeof(size_t*) * num_buffers);
	value_list = (char***)malloc(sizeof(char**) * num_buffers);
	flag_list = (int**)malloc(num_buffers);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		offset_list[i] = (uint64_t*)malloc(sizeof(size_t) 
			* store->max_keys_per_token);
		value_list[i] = (char**)malloc(sizeof(char*)
			* store->max_keys_per_token);
		flag_list[i] = (int*)calloc(store->max_keys_per_token, sizeof(int));
		for (int j=0; j<store->max_keys_per_token; j++) {
			value_list[i][j] = (char*)malloc(PAOSIZE);
		}
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

StoreHasher::~StoreHasher()
{
	uint64_t num_buffers = store->aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(offset_list[i]);
		for (int j=0; j<store->max_keys_per_token; j++) {
			free(value_list[i][j]);
		}
		free(value_list[i]);
		free(flag_list[i]);
		free(send[i]);
	}
	free(flag_list);
	free(offset_list);
	free(value_list);
	free(send);
}

void StoreHasher::findBinOffsetIndex(char* key, uint64_t& bkt)
{
	uint64_t hashv;
	Hash(key, strlen(key), store->store_size, hashv, bkt);
}

void* StoreHasher::operator()(void* pao_list)
{
	uint64_t ind = 0;
	PartialAgg *pao, *alloced_pao;
	char* pao_in_store;
	ssize_t n_read;
	uint64_t slot_index, orig_slot_index, slot_offset;
	uint64_t bin_index, bin_offset;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	// TODO: Remove!

	uint64_t* this_offset_list = offset_list[next_buffer];
	char** this_value_list = value_list[next_buffer];
	int* this_flag_list = flag_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	char* buf = (char*)malloc(BINSIZE);

	for (ind = 0; ind < recv_length; ind++) {
		pao = pao_l[ind];
//		printf("%p, %ld\n", value_list[0][ind], store->max_keys_per_token);

		// Find bin using hash function
		findBinOffsetIndex(pao->key, bin_index);
		assert(bin_index < store->store_size);

		// Find offset of value in external HT
		// TODO: Change this!
		int sum = 0;
		for (int i=0; i<3; i++) {
			sum += pao->key[i] << i;
		}
		slot_index = sum % SLOTS_PER_BIN;
		assert(slot_index >= 0);

		slot_offset = slot_index << PAOSIZE_BITS;
		bin_offset = bin_index << BINSIZE_BITS; 

//		cout << "Bin index: " << bin_index << endl;;

		/* If the (bin_index, slot_index) is unoccupied, then we simply add
		   the key and the combination to the forwarding buffers. This could
		   mean that more than one key is assigned to the same slot, which
		   will have to be handled by the StoreWriter */
		if (!store->slot_occupied(bin_index, slot_index)) 
			goto key_not_present;

		/* If the slot is occupied, then we know that a key exists at that 
		   offset in the external store. We then read a bin from that offset
		   and check if the key in the slot matches. If it does, we copy the
		   value from the external store into the value buffer for the 
		   Aggregator. If it doesn't we check if the following slots contain
		   the key. If we hit an empty slot while searching then we conclude
		   that the key doesn't exist. */

		// Read value from bin_offset
//		cout << "Bin offset: " << bin_offset << endl;
		n_read = pread64(store->store_fd, buf, BINSIZE, bin_offset);
		assert(n_read == BINSIZE);
		
		// Key is present; check if it's what we want
		assert(slot_offset < BINSIZE);
		pao_in_store = buf + slot_offset;

		orig_slot_index = slot_index;
		while (memcmp(pao->key, pao_in_store, strlen(pao->key))) {
			// Not the key we want; move to next slot
			slot_index = (slot_index + 1) % SLOTS_PER_BIN;
			// If slot is empty, our key is not present
			if (slot_index == orig_slot_index) {
				perror("Hashtable full");
				exit(0);
			}
			if (!store->slot_occupied(bin_index, slot_index))
				goto key_not_present;
		}
		// Key is present, and matches our key; copy
		memcpy(value_list[0][ind], pao_in_store, PAOSIZE);
		slot_offset = slot_index << PAOSIZE_BITS;
		this_offset_list[ind] = bin_offset + slot_offset;
		this_flag_list[ind] = 0;
		
		continue;
key_not_present:
		/* Set offset where StoreWriter must write to
		   and set flag to 1 to indicate new key */
		slot_offset = slot_index << PAOSIZE_BITS;
		this_offset_list[ind] = bin_offset + slot_offset;
		this_flag_list[ind] = 1;
	}

	this_send->result = pao_l;
	this_send->length = recv_length;
	this_send->result1 = this_value_list;
	this_send->result2 = this_offset_list;
	this_send->result5 = this_flag_list;
	return this_send;
}

StoreAggregator::StoreAggregator(Store* store) :
		filter(/*serial=*/true),
		store(store),
		rep_hash(NULL),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = store->aggregator->getNumBuffers();
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

StoreAggregator::~StoreAggregator()
{
	uint64_t num_buffers = store->aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(send[i]);
	}
	free(value_list);
	free(send);
}

void* StoreAggregator::operator()(void* pao_list)
{
	char *key, *value;
	uint64_t ind = 0;
	PartialAgg *pao, *found;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	
	char** recv_val_list = (char**)recv->result1;
	uint64_t* recv_off_list = (uint64_t*)recv->result2;

	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	for (ind=0; ind < recv_length; ind++) {
		pao = pao_l[ind];

		/* We maintain a hashtable to eliminate duplicates such that the
		 * writer sees only one instance of each key.
		 */
		found = NULL;
		HASH_FIND_STR(rep_hash, pao->key, found);
		if (found) {
			found->merge(pao);
			store->destroyPAO(pao);
			pao_l[ind] = NULL;
			continue;
		} else {
			HASH_ADD_KEYPTR(hh, rep_hash, pao->key, strlen(pao->key), pao);
		}
		// Aggregate values
		if (0)
			pao->add(recv_val_list[ind] + MAX_KEYSIZE);
	}
	HASH_CLEAR(hh, rep_hash);

	this_send->result = pao_l;
	this_send->length = recv_length;
	this_send->result1 = recv_val_list;
	this_send->result2 = recv_off_list;
	this_send->result5 = recv->result5;
	return this_send;
}

StoreWriter::StoreWriter(Store* store) :
		filter(/*serial=*/true),
		store(store),
		next_buffer(0),
		tokens_processed(0)
{
}

StoreWriter::~StoreWriter()
{
}

void* StoreWriter::operator()(void* pao_list)
{
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	
	char** recv_value_list = (char**)recv->result1;
	uint64_t* recv_off_list = (uint64_t*)recv->result2;
	int* recv_flag_list = (int*)recv->result5;

	for (ind=0; ind < recv_length; ind++) {
		if (pao_l[ind] == NULL)
			continue;
		pao = pao_l[ind];
		uint64_t wr_offset = recv_off_list[ind];
		/* Multiple keys may hash to the same slot, but this can
		 * happen only if each of the colliding keys is being entered
		 * into the store for the first time. This condition can be 
		 * detected by looking at the values list. */
		uint64_t slot_index = (recv_off_list[ind] & (BINSIZE-1)) 
				>> PAOSIZE_BITS;
		uint64_t bin_index = recv_off_list[ind] >> BINSIZE_BITS;
		if (recv_flag_list[ind] == 1 && 
				store->slot_occupied(bin_index, slot_index)) {
			uint64_t orig_slot_index = slot_index;
			while(store->slot_occupied(bin_index, slot_index)) {
				slot_index = (slot_index + 1) % SLOTS_PER_BIN;
				if (slot_index == orig_slot_index) {
					perror("Hashtable full");
					exit(0);
				}
			}
			wr_offset = (bin_index << BINSIZE_BITS) + 
					(slot_index << PAOSIZE_BITS);
		}
		size_t nwrit = pwrite64(store->store_fd, pao->key, PAOSIZE,
				wr_offset);
		store->set_slot_occupied(bin_index, slot_index);
		store->destroyPAO(pao);
		assert(nwrit == PAOSIZE);
	}
}
