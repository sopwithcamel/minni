#ifndef LIB_HASHTABLE_H
#define LIB_HASHTABLE_H

#include "PartialAgg.h"

class Hashtable {
  public:
    Hashtable() {}
    ~Hashtable() {}
    virtual void search(const char* key, PartialAgg*& found) = 0;
    /* Insert the key, value pair. Any evictions due to overflow will cause
     * the evicted entries to be written into evict_list and the number of
     * entries evicted will be returned. Memory for evict_list must be
     * allocated by caller */
    virtual size_t insert(const char* key, size_t key_len, PartialAgg* value, 
            PartialAgg** evict_list) = 0;
    /* Evict all entries from the hashtable into evict_list and return the 
     * number of elements evicted. Memory for evict_list must be allocated
     * by caller */
    virtual size_t evictAll(PartialAgg** evict_list) = 0;

    /* Call these to change the size of the hashtable dynamically. These
     * return false if the respective lower and upper limits are hit and
     * true otherwise.  */
    virtual bool reduceSize() = 0;
    virtual bool increaseSize() = 0;

  protected:
	size_t ht_size;
	size_t ht_capacity;
    size_t min_capacity;
    size_t max_capacity;
    size_t capacity_step;
};

#endif
