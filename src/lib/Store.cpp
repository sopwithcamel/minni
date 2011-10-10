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

	occup = (uint64_t*)malloc(sizeof(uint64_t) * store_size);
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

bool Store::slot_occupied(size_t bin_offset, size_t bin_index)
{
	uint64_t mask = 1 << bin_index;
	if (occup[bin_offset] & mask > 0)
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
	offset_list = (size_t**)malloc(sizeof(size_t*) * num_buffers);
	value_list = (char***)malloc(sizeof(char**) * num_buffers);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		offset_list[i] = (size_t*)malloc(sizeof(size_t) 
			* store->max_keys_per_token);
		value_list[i] = (char**)malloc(sizeof(char*)
			* store->max_keys_per_token);
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
		free(send[i]);
	}
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
	cout << "Entering StoreHasher" << endl;
	uint64_t ind = 0;
	PartialAgg *pao, *alloced_pao;
	char* pao_in_store;
	ssize_t n_read;
	uint64_t slot_index, orig_slot_index, slot_offset;
	uint64_t bin_index, bin_offset;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = 1000; //(uint64_t)recv->length;	// TODO: Remove!

	size_t* this_offset_list = offset_list[next_buffer];
	char** this_value_list = value_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	char* buf = (char*)malloc(BINSIZE);

	for (ind = 0; ind < recv_length; ind++) {
		pao = pao_l[ind];

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

		cout << "Bin index: " << bin_index << endl;;

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
		cout << "Bin offset: " << bin_offset << endl;
		n_read = pread64(store->store_fd, buf, BINSIZE, bin_offset);
		assert(n_read == BINSIZE);
		
		// Key is present; check if it's what we want
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
		memcpy(this_value_list[ind], pao_in_store, PAOSIZE);
		slot_offset = slot_index << PAOSIZE_BITS;
		this_offset_list[ind] = bin_offset + slot_offset;
		
		continue;
key_not_present:
		// Set value to NULL and set offset where StoreWriter must write to
		// Also set slot as occupied.
		this_value_list[ind] = NULL;
		slot_offset = slot_index << PAOSIZE_BITS;
		this_offset_list[ind] = bin_offset + slot_offset;
	}
	printf("\n");

	this_send->result = pao_l;
	this_send->length = recv_length;
	this_send->result1 = this_value_list;
	this_send->result2 = this_offset_list;
	return this_send;
}

StoreAggregator::StoreAggregator(Store* store) :
		filter(/*serial=*/true),
		store(store),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = store->aggregator->getNumBuffers();
	value_list = (char***)malloc(sizeof(char**) * num_buffers);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		value_list[i] = (char**)malloc(sizeof(char*)
			* store->max_keys_per_token);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

StoreAggregator::~StoreAggregator()
{
	uint64_t num_buffers = store->aggregator->getNumBuffers();
	for (int i=0; i<num_buffers; i++) {
		free(value_list[i]);
		free(send[i]);
	}
	free(value_list);
	free(send);
}

void* StoreAggregator::operator()(void* pao_list)
{
	cout << "Entering StoreAggregator" << endl;
	char *key, *value;
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = 1000; //(uint64_t)recv->length;	
	char** recv_val_list = (char**)recv->result1;
	size_t* recv_off_list = (size_t*)recv->result2;

	char** this_value_list = value_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	for (ind=0; ind < recv_length; ind++) {
		pao = pao_l[ind];
		// Aggregate values
		if (recv_val_list[ind] != NULL) {
			pao->add(recv_val_list[ind] + MAX_KEYSIZE);
		}
		ind++;
	}

	this_send->result = pao_l;
	this_send->length = recv_length;
	this_send->result1 = recv_off_list;
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
	cout << "Entering StoreWriter" << endl;
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = 1000; //(uint64_t)recv->length;	
	size_t* recv_off_list = (size_t*)recv->result1;

	while (ind < recv_length) {
		pao = pao_l[ind];
		// Write value at offset
		uint64_t slot_index = (recv_off_list[ind] & (BINSIZE-1)) >> PAOSIZE_BITS;
		uint64_t bin_index = recv_off_list[ind] >> BINSIZE_BITS;
		size_t nwrit = pwrite64(store->store_fd, pao->key, PAOSIZE, recv_off_list[ind]);
		store->set_slot_occupied(bin_index, slot_index);
		assert(nwrit == PAOSIZE);

		ind++;
	}
}
