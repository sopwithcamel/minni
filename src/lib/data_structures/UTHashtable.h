#ifndef LIB_UTHASHTABLE_H
#define LIB_UTHASHTABLE_H

#include "Hashtable.h"
#include "PartialAgg.h"
#include "uthash.h"

#define DEL_THRESHOLD		1000

class UTHashtable : 
    public Hashtable {
  public:
    UTHashtable(size_t capacity);
    ~UTHashtable();
    void search(const char* key, PartialAgg*& found);
    /* Insert the key, value pair. Any evictions due to overflow will cause
     * the evicted entries to be written into evict_list and the number of
     * entries evicted will be returned. Memory for evict_list must be
     * allocated by caller */
    size_t insert(const char* key, size_t key_len, PartialAgg* value, 
            PartialAgg** evict_list);
    /* Evict all entries from the hashtable into evict_list and return the 
     * number of elements evicted. Memory for evict_list must be allocated
     * by caller */
    size_t evictAll(PartialAgg** evict_list);

  private:
	PartialAgg* hashtable;
};

#endif
