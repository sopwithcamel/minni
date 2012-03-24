#include "UTHashtable.h"
#ifdef UTHASH

UTHashtable::UTHashtable(size_t capacity) :
    hashtable(NULL)
{
    ht_size = 0;
    ht_capacity = capacity;
    min_capacity = ht_capacity / 10;
    max_capacity = ht_capacity * 2;
    capacity_step = ht_capacity / 100;
}

UTHashtable::~UTHashtable()
{
	PartialAgg *s, *tmp;
	HASH_ITER(hh, hashtable, s, tmp) {
		HASH_DEL(hashtable, s);
		free(s);
	}
}

void UTHashtable::search(const char* key, PartialAgg*& found)
{
		HASH_FIND_STR(hashtable, key, found);
}

size_t UTHashtable::insert(const char* key, size_t key_len, PartialAgg* value,
        PartialAgg** evict_list)
{
    size_t evict_list_ctr = 0;
    HASH_ADD_KEYPTR(hh, hashtable, key, key_len, value);
    ht_size++;
    if (ht_size > ht_capacity + DEL_THRESHOLD) { // PAO has to be evicted
        PartialAgg *entry, *tmp;
        HASH_ITER(hh, hashtable, entry, tmp) {
            evict_list[evict_list_ctr++] = entry;
            HASH_DEL(hashtable, entry);
            ht_size--;
            if (ht_size < ht_capacity)
                break;
        }
    }
    return evict_list_ctr;
}

bool UTHashtable::evictAll(PartialAgg** evict_list, size_t& num_evicted, 
        size_t max)
{
    PartialAgg *s, *tmp;
    bool retDone = true;
    size_t evict_list_ctr = 0;
    HASH_ITER(hh, hashtable, s, tmp) {
        evict_list[evict_list_ctr++] = s;
        HASH_DEL(hashtable, s);
        if (evict_list_ctr == max) {
            retDone = false;
            break;
        }
    }	
    ht_size -= evict_list_ctr;
    num_evicted = evict_list_ctr;
    return retDone;
}
#endif
