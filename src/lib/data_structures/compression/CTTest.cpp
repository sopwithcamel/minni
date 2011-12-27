#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "CompressTree.h"

int main()
{
    compresstree::CompressTree* ct = new compresstree::CompressTree(2, 8);
    char* buf = (char*)malloc(100);
    strcpy(buf, "testing");
    for (int i=0; i<1000; i++) {
        assert(ct->insert(i, buf, strlen(buf)));
    }
    assert(ct->flushBuffers());
    
    size_t buf_size;
    char* read_buf;
    uint64_t read_hash;
    while (true) {
        bool ret = ct->nextValue(read_hash, read_buf, buf_size);
        fprintf(stderr, "Read hash: %lu, size: %lu\n", read_hash, buf_size);
        if (!ret)
            break;
    }
}
