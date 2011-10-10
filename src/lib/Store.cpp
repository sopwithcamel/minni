#include "Store.h"

Store::Store(Aggregator* agg, 
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) :
		aggregator(agg),
		destroyPAO(destroyPAOFunc),
		max_keys_per_token(max_keys),
		next_buffer(0),
		tokens_processed(0)
{
	hasher = new StoreHasher(this);
	agger = new StoreAggregator(this);
	writer = new StoreWriter(this);
}

Store::~Store()
{
}

bool Store::key_present(size_t bin_offset, size_t bin_index)
{
	// TODO: Implement!
	return false;
}

void Store::set_key_present(size_t bin_offset, size_t bin_index)
{
	// TODO: Implement
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
			value_list[i][j] = (char*)malloc(VALUE_SIZE);
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

uint64_t StoreHasher::findBinOffset(char* key)
{
	// TODO: Implement
	return 0;
}

void* StoreHasher::operator()(void* pao_list)
{
	char *key, *value, *offset;
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	

	size_t* this_offset_list = offset_list[next_buffer];
	char** this_value_list = value_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	uint64_t num_paos_in_bin = BINSIZE / PAOSIZE;
	char* buf = (char*)malloc(BINSIZE);

	for (ind = 0; ind < recv_length; ind++) {
		pao = pao_l[ind];

		// Find bin using hash function
		uint64_t bin_offset = findBinOffset(pao->key);
		assert(bin_offset <= store->store_size - BINSIZE);

		// Read value
		ssize_t n_read = pread64(store->store_fd, buf, BINSIZE, bin_offset);
		
		// Find offset of value in external HT
		// TODO: Change this!
		int sum = 0;
		for (int i=0; i<3; i++) {
			sum += pao->key[i] << i;
		}
		size_t bin_ind = sum % num_paos_in_bin;
		if (!store->key_present(bin_offset, bin_ind)) { // key not present
			goto key_not_present;
		}

		// Key is present; check if it's what we want
		offset = buf + bin_ind * PAOSIZE;
		while (memcmp(pao->key, offset, strlen(pao->key))) {
			// Not the key we want; move to next slot
			bin_ind = (bin_ind + 1) % num_paos_in_bin;
			// If slot is empty, our key is not present
			if (!store->key_present(bin_offset, bin_ind))
				goto key_not_present;
		}
		// Key is present, and matches our key; copy
		memcpy(this_value_list[ind], offset, VALUE_SIZE);
		this_offset_list[ind] = bin_offset + bin_ind * PAOSIZE;
		
		continue;
key_not_present:
		// Set value to NULL and set offset where StoreWriter must write to
		// Also set slot as occupied.
		this_value_list[ind] = NULL;
		this_offset_list[ind] = bin_offset + bin_ind * PAOSIZE;
		store->set_key_present(bin_offset, bin_ind);
		
	}

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
	char *key, *value;
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	
	char** recv_val_list = (char**)recv->result1;
	size_t* recv_off_list = (size_t*)recv->result2;

	char** this_value_list = value_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % store->aggregator->getNumBuffers();

	for (ind=0; ind < recv_length; ind++) {
		pao = pao_l[ind];
		// Aggregate values
		pao->add(recv_val_list[ind] + MAX_KEYSIZE);
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
	uint64_t ind = 0;
	PartialAgg* pao;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	
	size_t* recv_off_list = (size_t*)recv->result1;

	while (ind < recv_length) {
		pao = pao_l[ind];
		// Write value at offset
		pwrite64(store->store_fd, pao->key, PAOSIZE, recv_off_list[ind]);
		ind++;
	}
}
