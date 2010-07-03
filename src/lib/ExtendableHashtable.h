#ifndef LIB_EXTENDABLEHASHTABLE_H
#define LIB_EXTENDABLEHASHTABLE_H

#include <iostream>
#include <fstream>
#include <string>
#include <map>

#include <stdint.h>

using namespace std;

#include "PartialAgg.h"

class ExtendableHashtable {
	public:
		ExtendableHashtable(uint64_t capacity); /* constructor */
		ExtendableHashtable(uint64_t capacity, const string &path); /* load constructor, from a "dump" file */
		~ExtendableHashtable(); /* destructor */
		PartialAgg insert(string key, PartialAgg pao); /* returns PAO from map, or pao if not in map already */
		PartialAgg get(const string &key); /* returns matching PAO or null on miss */
		bool finalize(const string &path); /* true on success, false otherwise */

	private:
		map<string, PartialAgg> hashtable;
		uint64_t capacity; /* maximum capacity of map before dumping */
		uint64_t dumpNumber; /* monotonically increasing dump sequence number */
		string dumpFile; /* current dumpFile on disk */
		bool dumpHashtable(string fname); /* true if dump succeeds, false otherwise */

};

#endif
