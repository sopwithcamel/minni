#ifndef LIB_EHFULLEVICT_H
#define LIB_EHFULLEVICT_H

#include <iostream>
#include <fstream>
#include <string>
#include <tr1/unordered_map>
#include <iterator>

#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

using namespace std;

#include "PartialAgg.h"
#define GetCurrentDir getcwd

#define REGULAR_SERIALIZE	0
#define NSORT_SERIALIZE		1

typedef std::tr1::unordered_map<string, PartialAgg*> Hash;

class EHFullEvict {
	public:
		EHFullEvict(uint64_t capacity, uint64_t partid); /* constructor */
		EHFullEvict(uint64_t capacity, uint64_t partid, const string &path); /* load constructor, from a "dump" file */
		~EHFullEvict(); /* destructor */
		Hash::iterator find(const string &key); /* returns matching PAO or null on miss */
		bool add(const string &key, const string &value); /* add to existing PAO */
		bool finalize(string fname); /* true on success, false otherwise */
		bool clear();
		void setSerializeFormat(int);

	private:
		Hash hashtable;
		bool regularSerialize; /* Regular serialize? */
		uint64_t partid;
		uint64_t capacity; /* maximum capacity of map before dumping */
		uint64_t dumpNumber; /* monotonically increasing dump sequence number */
		string evictFileName; /* current dumpFile on disk */
		FILE* evictFile;
		Hash::iterator beg;

		bool insert(string key, PartialAgg* pao); /* returns PAO from map, or pao if not in map already */
		bool dumpHashtable(); /* true if dump succeeds, false otherwise */
		bool evict();
		string getDumpFileName(uint64_t);
		void serialize(FILE*, string, string);
		void serialize(FILE*, char, uint64_t, const char*, uint64_t, const char*);
		int deSerialize(FILE*, char*, uint64_t*, char**, uint64_t*, char**);

};

typedef EHFullEvict MapperAggregator;

#endif
