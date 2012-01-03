#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <tr1/unordered_map>
#include <google/sparse_hash_map>
#include "snappy.h"

using namespace std;

#define SNAPPY

void gen_random(string& s, const int len)
{
    for (int i = 0; i < len; ++i) {
        int randomChar = rand()%(26+26+10);
        if (randomChar < 26)
            s[i] = 'a' + randomChar;
        else if (randomChar < 26+26)
            s[i] = 'A' + randomChar - 26;
        else
            s[i] = '0' + randomChar - 26 - 26;
    }
    s[len] = 0;
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        perror("Input number of values to insert\n");
        exit(0);
    }
    size_t num_insert = atoi(argv[1]);
    fprintf(stderr, "Testing insertion of %ld entries\n", num_insert);
    string buf = "Add a new child to the node; the child type indicates which side aslkasndlaksdalksdnalskdnasldasd";
#ifdef SNAPPY
    string output;
    snappy::Compress(buf.data(), buf.size(), &output);
    cout << output.size() << endl;
#endif
    srand(56);
//    typedef tr1::unordered_map<int, char*> Hash;
    typedef google::sparse_hash_map<int64_t, string> Hash;
    Hash hash;
    hash.set_deleted_key(-1);
    time_t stime = time(NULL);
    for (size_t i=0; i<num_insert; i++) {
        gen_random(buf, rand() % 100);
#ifdef SNAPPY
        snappy::Compress(buf.data(), buf.size(), &output);
        hash[rand()] = output;
#else
        hash[rand()] = buf;
#endif
    }
    time_t ftime = time(NULL);
    fprintf(stderr, "finished insertion %ld\n", ftime - stime);
    getchar();
}
