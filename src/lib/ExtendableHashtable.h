#ifndef LIB_EXTENDABLEHASHTABLE_H
#define LIB_EXTENDABLEHASHTABLE_H

#include <iostream>
#include <fstream>
#include <string>
#include <tr1/unordered_map>

#include <stdint.h>
#include <stdlib.h>

using namespace std;

#include "PartialAgg.h"
#define GetCurrentDir getcwd

typedef std::tr1::unordered_map<string, PartialAgg*> Hash;

class ExtendableHashtable {
	public:
		ExtendableHashtable(uint64_t capacity, uint64_t partid); /* constructor */
		ExtendableHashtable(uint64_t capacity, uint64_t partid, const string &path); /* load constructor, from a "dump" file */
		~ExtendableHashtable(); /* destructor */
		Hash::iterator find(const string &key); /* returns matching PAO or null on miss */
		bool add(const string &key, const string &value); /* add to existing PAO */
		bool finalize(string fname); /* true on success, false otherwise */
		bool clear();

	private:
		Hash hashtable;
		uint64_t partid;
		uint64_t capacity; /* maximum capacity of map before dumping */
		uint64_t dumpNumber; /* monotonically increasing dump sequence number */
		string dumpFile; /* current dumpFile on disk */

		bool insert(string key, PartialAgg* pao); /* returns PAO from map, or pao if not in map already */
		bool dumpHashtable(string fname); /* true if dump succeeds, false otherwise */
		string getDumpFileName(uint64_t);
		void serialize(FILE*, char, uint64_t, const char*, uint64_t, const char*);
		int deSerialize(FILE*, char*, uint64_t*, char**, uint64_t*, char**);

};

typedef ExtendableHashtable MapperAggregator;

#endif
