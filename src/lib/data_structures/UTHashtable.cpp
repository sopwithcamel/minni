#include "UTHashtable.h"

UTHashtable::UTHashtable(size_t capacity) :
    hashtable(NULL),
    ht_size(0),
    Hashtable(capacity)
{
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

size_t UTHashtable::insert(const char* key, size_t key_len, PartialAgg* value.
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

size_t UTHashtable::evictAll(PartialAgg** evict_list)
{
    PartialAgg *s, *tmp;
    size_t evict_list_ctr = 0;
    HASH_ITER(hh, hashtable, s, tmp) {
        evict_list[evict_list_ctr++] = s;
    }	
    HASH_CLEAR(hh, hashtable);
    ht_size = 0;
}

bool UTHashtable::reduceSize()
{
    if (ht_capacity - capacity_step < min_capacity)
        return false;
    ht_capacity -= capacity_step;
    return true;
}

bool UTHashtable::increaseSize()
{
    if (ht_capacity + capacity_step > max_capacity)
        return false;
    ht_capacity += capacity_step;
    return true;
}

