#ifndef LIB_EHFAWN_H
#define LIB_EHFAWN_H

#include <iostream>
#include <fstream>
#include <string>
#include <tr1/unordered_map>
#include <set>
#include <iterator>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

using namespace std;

#include "PartialAgg.h"
#include "fawnds.h"
#include "fawnds_flash.h"
#include "hashutil.h"
#include "dbid.h"

using namespace fawn;
using fawn::HashUtil;

#define GetCurrentDir getcwd

#define REGULAR_SERIALIZE	0
#define NSORT_SERIALIZE		1

#define EVICT_CACHE_SIZE	1024

typedef std::tr1::unordered_map<string, PartialAgg*> Hash;

class EHFawn {
	public:
		EHFawn(uint64_t capacity, uint64_t partid); /* constructor */
		EHFawn(uint64_t capacity, uint64_t partid, const string &path); /* load constructor, from a "dump" file */
		~EHFawn(); /* destructor */
		Hash::iterator find(const string &key); /* returns matching PAO or null on miss */
		bool add(const string &key, const string &value); /* add to existing PAO */
		bool finalize(string fname);
		bool finalize(); 
		bool clear();
		void setSerializeFormat(int);

		uint64_t evict_ctr;
		uint64_t insert_ctr;
		uint64_t beg_ctr;
		uint64_t found_in_fds_ctr;

	private:
		Hash hashtable;
		bool regularSerialize; /* Regular serialize? */
		uint64_t partid;
		uint64_t capacity; /* maximum capacity of map before dumping */
		uint64_t dumpNumber; /* monotonically increasing dump sequence number */
		Hash::iterator beg;
		string evictHashName;
		FawnDS<FawnDS_Flash> *evictHash;
		FILE *evictFile;
		tr1::unordered_map<string, bool> bufferedKeys;

		bool insert(string key, PartialAgg* pao); /* returns PAO from map, or pao if not in map already */
		bool evict();
		void merge(Hash::iterator it);
		void serialize(FILE*, char, uint64_t, const char*, uint64_t, const char*);
		void serialize(FILE*, string, string);
		int deSerialize(FILE*, char*, uint64_t*, char**, uint64_t*, char**);

};

typedef EHFawn MapperAggregator;

#endif
