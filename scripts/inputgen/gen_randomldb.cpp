#include <iostream>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <vector>
#include "tbb/task.h"
#include "tbb/tbb_thread.h"
#include <unistd.h>
#include <sstream>
#include <fstream>

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/cache.h"

using namespace std;

/* Configuration */
long UNIQ_KEYS;
long LEN;
long REPEAT_FACTOR;
/* Config ends */

#define CONV_26(x)      x * log(2)/log(26)

int main(int argc, char* argv[])
{
        UNIQ_KEYS = 20000000;
        LEN = 200;

        char ht_name[100] = "/localfs/hamur/randwords";

        leveldb::Options options;
        leveldb::DB* db;
        options.create_if_missing = true;
        options.block_cache = leveldb::NewLRUCache(100 * 1048576);
        //	options.write_buffer_size = ;
        leveldb::Status s = leveldb::DB::Open(options, ht_name, &db);
        assert(s.ok());

        char* word = (char*)malloc(LEN + 1);
        word[LEN] = '\0';
        char* ind = (char*)malloc(20);

        for(long i=0; i<UNIQ_KEYS; i++) {
            for (int j=0; j<LEN; j++) {
                word[j] = 97 + rand() % 26;
            }          
            leveldb::Slice wrd = word;
            sprintf(ind, "%ld", i);
            leveldb::Slice k = ind;
            leveldb::Status s = db->Put(leveldb::WriteOptions(), k, wrd);
            assert(s.ok());
        }
        delete db;
        free(word);
}
                 
