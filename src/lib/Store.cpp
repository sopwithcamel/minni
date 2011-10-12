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
	store_fd = open("/mnt/hamur/store.ht", O_CREAT|O_RDWR|O_NOATIME, 0666);
//	posix_fallocate(store_fd, 0, store_size * BINSIZE);
	assert(store_fd > 0);

	occup = (uint64_t*)calloc(store_size, sizeof(uint64_t));
	assert(occup != NULL);
	for (int i=0; i<store_size; i++)
		assert(occup[i] == 0);

	uint64_t num_buffers = aggregator->getNumBuffers();

	pao_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers);
	list_length = (uint64_t*)malloc(sizeof(uint64_t) * num_buffers);
	offset_list = (uint64_t**)malloc(sizeof(size_t*) * num_buffers);
	value_list = (char***)malloc(sizeof(char**) * num_buffers);
	flag_list = (int**)malloc(num_buffers);

	for (int i=0; i<num_buffers; i++) {
		offset_list[i] = (uint64_t*)malloc(sizeof(size_t) 
			* max_keys_per_token);
		value_list[i] = (char**)malloc(sizeof(char*)
			* max_keys_per_token);
		flag_list[i] = (int*)calloc(max_keys_per_token, sizeof(int));
		for (int j=0; j<max_keys_per_token; j++) {
			value_list[i][j] = (char*)malloc(PAOSIZE);
		}
	}
}

Store::~Store()
{
	delete hasher;
	delete agger;
	delete writer;
	close(store_fd);
	free(occup);

	uint64_t num_buffers = aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(offset_list[i]);
		for (int j=0; j<max_keys_per_token; j++) {
			free(value_list[i][j]);
		}
		free(value_list[i]);
		free(flag_list[i]);
		free(pao_list[i]);
	}
	free(list_length);
	free(flag_list);
	free(offset_list);
	free(value_list);
}

bool Store::slot_occupied(size_t bin_index, size_t slot_index)
{
	uint64_t a = 1;
	uint64_t mask = (a << slot_index);
	if ((occup[bin_index] & mask) > 0) {
		return true;
	}
	return false;
}

void Store::set_slot_occupied(size_t bin_index, size_t slot_index)
{
	uint64_t a = 1;
	uint64_t mask = (a << slot_index);
//	printf("Set: %llu, %llu, %ld, %ld\n", occup[bin_index], mask, bin_index, slot_index);
	occup[bin_index] |= mask;
//	cout << "After: " << occup[bin_index] << endl;
}


StoreHasher::StoreHasher(Store* store) :
		filter(/*serial=*/true),
		store(store),
		next_buffer(0),
		tokens_processed(0)
{
}

StoreHasher::~StoreHasher()
{
}

void StoreHasher::findBinOffsetIndex(char* key, uint64_t& bkt)
{
	uint64_t hashv;
	Hash(key, strlen(key), store->store_size, hashv, bkt);
}

void* StoreHasher::operator()(void* pao_list)
{
	cout << "Entering StoreHasher" << endl;
	uint64_t ind = 0;
	PartialAgg *pao, *alloced_pao;
	char* pao_in_store;
	ssize_t n_read;
	uint64_t slot_index, orig_slot_index, slot_offset;
	uint64_t bin_index, bin_offset;

	FilterInfo* recv = (FilterInfo*)pao_list;
	// Set the pointers in store to what we received
	store->pao_list[next_buffer] = (PartialAgg**)recv->result;
	store->list_length[next_buffer] = (uint64_t)recv->length;

	PartialAgg** this_pao_list = store->pao_list[next_buffer];
	uint64_t this_length = store->list_length[next_buffer];
	assert(this_length < store->max_keys_per_token);

	uint64_t* this_offset_list = store->offset_list[next_buffer];
	char** this_value_list = store->value_list[next_buffer];
	int* this_flag_list = store->flag_list[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	char* buf = (char*)malloc(BINSIZE);

	for (ind = 0; ind < this_length; ind++) {
		pao = this_pao_list[ind];
//		strcpy(pao->key, "aaaaaaa");
//		printf("%p, %ld\n", value_list[0][ind], store->max_keys_per_token);

		// Find bin using hash function
		findBinOffsetIndex(pao->key, bin_index);
		assert(bin_index < store->store_size);

		// Find offset of value in external HT
		// TODO: Change this!
		int sum = 0;
		for (int i=0; i<3; i++) {
			sum += (pao->key[i] << i);
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
//		cout << bin_index << ", " << slot_index << ", " << bin_offset << endl;
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
			assert(slot_index != orig_slot_index);
			if (!store->slot_occupied(bin_index, slot_index))
				goto key_not_present;
		}
		// Key is present, and matches our key; copy
		memcpy(this_value_list[ind], pao_in_store, PAOSIZE);
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
}

StoreAggregator::StoreAggregator(Store* store) :
		filter(/*serial=*/true),
		store(store),
		rep_hash(NULL),
		next_buffer(0),
		tokens_processed(0)
{
}

StoreAggregator::~StoreAggregator()
{
}

void* StoreAggregator::operator()(void* pao_list)
{
	cout << "Entering StoreAggregator" << endl;
	uint64_t ind = 0;
	PartialAgg *pao, *found;

	PartialAgg** this_pao_list = store->pao_list[next_buffer];
	uint64_t this_length = store->list_length[next_buffer];
	char** this_value_list = store->value_list[next_buffer];
	int* this_flag_list = store->flag_list[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	for (ind=0; ind < this_length; ind++) {
		pao = this_pao_list[ind];

		/* We maintain a hashtable to eliminate duplicates such that the
		 * writer sees only one instance of each key.
		 */
		found = NULL;
		HASH_FIND_STR(rep_hash, pao->key, found);
		if (found) {
			found->merge(pao);
			store->destroyPAO(pao);
			this_pao_list[ind] = NULL;
			continue;
		} else {
			HASH_ADD_KEYPTR(hh, rep_hash, pao->key, strlen(pao->key), pao);
		}
		// Aggregate values
		if (this_flag_list[ind] == 0)
			pao->add(this_value_list[ind] + MAX_KEYSIZE);
	}
	HASH_CLEAR(hh, rep_hash);
}

StoreWriter::StoreWriter(Store* store) :
		filter(/*serial=*/true),
		store(store),
		next_buffer(0),
		tokens_processed(0)
{
	empty_bin = (char*)calloc(BINSIZE, 1);
}

StoreWriter::~StoreWriter()
{
	free(empty_bin);
}

void* StoreWriter::operator()(void* pao_list)
{
	cout << "Entering StoreWriter" << endl;
	uint64_t ind = 0;
	PartialAgg* pao;
	size_t n_writ;

	PartialAgg** this_pao_list = store->pao_list[next_buffer];
	uint64_t this_length = store->list_length[next_buffer];
	uint64_t* this_offset_list = store->offset_list[next_buffer];
	char** this_value_list = store->value_list[next_buffer];
	int* this_flag_list = store->flag_list[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	for (ind=0; ind < this_length; ind++) {
		if (this_pao_list[ind] == NULL)
			continue;
		pao = this_pao_list[ind];
		// the offset to write at
		uint64_t wr_offset = this_offset_list[ind];
		// number of bytes to write
		size_t wr_size = PAOSIZE;
		// buffer to write
		char* wr_buf = pao->key;

		/* Multiple keys may hash to the same slot, but this can
		 * happen only if each of the colliding keys is being entered
		 * into the store for the first time. This condition can be 
		 * detected by looking at the values list. */
		uint64_t slot_offset = (this_offset_list[ind] & (BINSIZE-1));
		uint64_t slot_index = (slot_offset >> PAOSIZE_BITS);
		uint64_t bin_index = this_offset_list[ind] >> BINSIZE_BITS;
		uint64_t bin_offset = bin_index << BINSIZE_BITS;

	//	cout << bin_index << ", " << slot_index << endl;
		if (this_flag_list[ind] == 1 && 
				store->slot_occupied(bin_index, slot_index)) {
			/* this key isn't present in the bin, but another key
			   occupies the slot that it hashes to, so we need to
			   find another slot for it */
			uint64_t orig_slot_index = slot_index;
			do {
				slot_index = (slot_index + 1) % SLOTS_PER_BIN;
				if (slot_index == orig_slot_index)
					cout << " FML (" << bin_index << ", " << slot_index << ")" << endl;
				assert(slot_index != orig_slot_index);
			} while(store->slot_occupied(bin_index, slot_index));
			// we have changed the location of the write, so update
			wr_offset = bin_offset + (slot_index << PAOSIZE_BITS);
//			cout << " (" << bin_index << ", " << slot_index << ")" << endl;
		} else if (store->occup[bin_index] == 0) {
			/* If the bin has never been written to then we fill
			   the rest of the bin with zeroes */
			memset(empty_bin, 0, BINSIZE);
			memcpy(empty_bin + slot_offset, pao->key, PAOSIZE);
			wr_offset = (bin_index << BINSIZE_BITS);
			wr_buf = empty_bin;
			wr_size = BINSIZE;
		}
		this_flag_list[ind] = 0;

//		cout << "Writing " << pao->key  << ", " << pao->value;
//		cout << ", " << wr_size << " bytes" << " at " << wr_offset;
//		cout << " (" << bin_index << ", " << slot_index << "): " << pao->key << endl;
		n_writ = pwrite64(store->store_fd, wr_buf, wr_size, wr_offset);
		assert(n_writ == wr_size);
		store->set_slot_occupied(bin_index, slot_index);
		store->destroyPAO(pao);
	}
}
