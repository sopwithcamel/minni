#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "CompressTree.h"

int testMonotonicIncrease(uint32_t a, uint32_t b)
{
    size_t i, numIns=1000;
    compresstree::CompressTree* ct = new compresstree::CompressTree(a, b);
    fprintf(stderr, "Testing insertion of monotonically increasing values\n");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=0; i<numIns; i++) {
        assert(ct->insert(i, buf, strlen(buf) + 1));
    }
    fprintf(stderr, "Number of values inserted: %lu\n", i);
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
    fprintf(stderr, "Number of values read: %lu\n", readValues);
    if (numIns != readValues)
        goto error;
    delete ct;
    return true;
error:
    delete ct;
    return false;
}

int testMonotonicDecrease(uint32_t a, uint32_t b)
{
    size_t i, numIns = 1000;
    compresstree::CompressTree* ct = new compresstree::CompressTree(a, b);
    fprintf(stderr, "Testing insertion of monotonically decreasing values\n");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=numIns; i>0; i--) {
        assert(ct->insert(i, buf, strlen(buf) + 1));
    }
    fprintf(stderr, "Number of values inserted: %lu\n", numIns);
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
    fprintf(stderr, "Number of values read: %lu\n", readValues);
    delete ct;
    return true;
}

int testRandom(uint32_t a, uint32_t b)
{
    size_t i, numIns = 1000;
    compresstree::CompressTree* ct = new compresstree::CompressTree(a, b);
    fprintf(stderr, "Testing insertion of random values\n");
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (i=0; i<numIns; i++) {
        assert(ct->insert(rand(), buf, strlen(buf) + 1));
    }
    fprintf(stderr, "Number of values inserted: %lu\n", numIns);
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
    fprintf(stderr, "Number of values read: %lu\n", readValues);
    delete ct;
    return true;
}

int main()
{
//    assert(testMonotonicIncrease(2, 8));
//    assert(testMonotonicDecrease(2, 8));
    assert(testRandom(2, 8));
}
