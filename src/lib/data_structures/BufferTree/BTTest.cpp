#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <google/heap-profiler.h>

#include "BufferTree.h"

int testMonotonicIncrease(uint32_t a, uint32_t b, size_t num_insert)
{
    size_t i;
    buffertree::BufferTree* ct = new buffertree::BufferTree(a, b);
    fprintf(stderr, "Testing insertion of monotonically increasing values... ");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=0; i<num_insert; i++) {
        assert(ct->insert(i, buf, strlen(buf) + 1));
    }
    assert(ct->flushBuffers());
    
    size_t buf_size;
    char* read_buf;
    uint64_t read_hash;
    size_t readValues = 0;
    while (true) {
        bool ret = ct->nextValue(read_hash, read_buf, buf_size);
//        fprintf(stderr, "Read hash: %lu, buf: %s, size: %lu\n", read_hash, read_buf, buf_size);
        readValues++;
        if (!ret)
            break;
    }
    if (readValues == num_insert)
        fprintf(stderr, "passed\n");
    else {
        fprintf(stderr, "failed; Ins: %ld, Read: %ld\n", num_insert, readValues);
    }
    delete ct;
    return true;
}

int testMonotonicDecrease(uint32_t a, uint32_t b, size_t num_insert)
{
    size_t i;
    buffertree::BufferTree* ct = new buffertree::BufferTree(a, b);
    fprintf(stderr, "Testing insertion of monotonically decreasing values... ");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=num_insert; i>0; i--) {
        assert(ct->insert(i, buf, strlen(buf) + 1));
    }
    assert(ct->flushBuffers());
    
    size_t buf_size;
    char* read_buf;
    uint64_t read_hash;
    size_t readValues = 0;
    while (true) {
        bool ret = ct->nextValue(read_hash, read_buf, buf_size);
//        fprintf(stderr, "Read hash: %lu, buf: %s, size: %lu\n", read_hash, read_buf, buf_size);
        readValues++;
        if (!ret)
            break;
    }
    if (readValues == num_insert)
        fprintf(stderr, "passed\n");
    delete ct;
    return true;
}

void gen_random(char* s, const int len)
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

int testRandom(uint32_t a, uint32_t b, size_t num_insert)
{
    size_t i;
    buffertree::BufferTree* ct = new buffertree::BufferTree(a, b);
    srand(56);
    fprintf(stderr, "Testing insertion of %ld random values... ", num_insert);
    char* buf = (char*)malloc(100);
    strcpy(buf, "Add a new child to the node; the child type indicates which side");
//    sprintf(buf, "%ld", rand());
    time_t stime = time(NULL);
    for (i=0; i<num_insert; i++) {
//        fprintf(stderr, "Inserted: %lu\n", hash);
        gen_random(buf, rand() % 100);
        uint64_t hash = rand();
        assert(ct->insert(hash, buf, strlen(buf) + 1));
    }
    assert(ct->flushBuffers());
    time_t ftime = time(NULL);
    fprintf(stderr, "inserted in %lds... ", ftime - stime);
    
    size_t buf_size;
    char* read_buf;
    uint64_t read_hash;
    size_t readValues = 0;
    while (true) {
        bool ret = ct->nextValue(read_hash, read_buf, buf_size);
//        fprintf(stderr, "Read hash: %lu, buf: %s, size: %lu\n", read_hash, read_buf, buf_size);
        readValues++;
        if (!ret)
            break;
    }
    getchar();
    fprintf(stderr, "passed\n");
    delete ct;
    return true;
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        perror("Input number of elements to insert\n");
        exit(1);
    }
    size_t num_insert = atoi(argv[1]);
    assert(testMonotonicIncrease(2, 8, num_insert));
//    assert(testMonotonicDecrease(2, 8));
//    assert(testRandom(2, 4, 1));
//    assert(testRandom(2, 8, num_insert));
}
