#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "CompressTree.h"

int testMonotonicIncrease(uint32_t a, uint32_t b)
{
    size_t i, numIns=1000000;
    compresstree::CompressTree* ct = new compresstree::CompressTree(a, b, compresstree::SNAPPY);
    fprintf(stderr, "Testing insertion of monotonically increasing values... ");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=0; i<numIns; i++) {
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
    if (readValues == numIns)
        fprintf(stderr, "passed\n");
    else {
        fprintf(stderr, "failed; Ins: %ld, Read: %ld\n", numIns, readValues);
    }
    delete ct;
    return true;
}

int testMonotonicDecrease(uint32_t a, uint32_t b)
{
    size_t i, numIns = 1000;
    compresstree::CompressTree* ct = new compresstree::CompressTree(a, b, compresstree::SNAPPY);
    fprintf(stderr, "Testing insertion of monotonically decreasing values... ");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=numIns; i>0; i--) {
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
    if (readValues == numIns)
        fprintf(stderr, "passed\n");
    delete ct;
    return true;
}

int testRandom(uint32_t a, uint32_t b, size_t numIns)
{
    size_t i;
    compresstree::CompressTree* ct = new compresstree::CompressTree(a, b, compresstree::SNAPPY);
    srand(56);
    fprintf(stderr, "Testing insertion of %ld random values... ", numIns);
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
//    sprintf(buf, "%ld", rand());
    time_t stime = time(NULL);
    for (i=0; i<numIns; i++) {
        uint64_t hash = rand();
//        fprintf(stderr, "Inserted: %lu\n", hash);
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
    char c = getchar();
    fprintf(stderr, "passed\n");
    delete ct;
    return true;
}

int main()
{
//    assert(testMonotonicIncrease(2, 8));
//    assert(testMonotonicDecrease(2, 8));
//    assert(testRandom(2, 4, 1));
    assert(testRandom(2, 8, 10000000));
}
