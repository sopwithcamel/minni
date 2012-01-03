#ifndef LIB_COMPRESS_COMPRESSTREE_H
#define LIB_COMPRESS_COMPRESSTREE_H

#include <queue>
#include "Node.h"

namespace compresstree {

    const size_t BUFFER_SIZE = 10485760;
//    const size_t BUFFER_SIZE = 20971520;
    const size_t EMPTY_THRESHOLD = BUFFER_SIZE / 2;
    const size_t MAX_ELS_PER_BUFFER = BUFFER_SIZE / 16;

    enum CompressAlgorithm {
        SNAPPY,
        ZLIB
    };
    
    class Node;

    class CompressTree {
        friend class Node;
      public:
        CompressTree(uint32_t a, uint32_t b, CompressAlgorithm alg);
        ~CompressTree();

        /* Insert record into tree */
        bool insert(uint64_t hash, void* buf, size_t buf_size);
        /* Write out all buffers to leaves. Do this before reading */
        bool flushBuffers();
        /* read values */
        bool nextValue(uint64_t& hash, char*& buf, size_t& buf_size);
      private:
        /* Add leaf whose buffer is full to be emptied once all internal node
         * buffers have been emptied */
        bool addLeafToEmpty(Node* node);
        size_t handleFullLeaves();
        bool createNewRoot(Node* otherChild);
      public:
        // (a,b)-tree...
        const uint32_t a_;
        const uint32_t b_;
      private:
        CompressAlgorithm alg_;
        Node* rootNode_;
        std::queue<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        size_t lastLeafRead_;
        size_t lastOffset_;
        /* Node related */
        uint64_t** els_;         // pointers to elements in buffer
        char* auxBuffer_;       // used in sorting
        char* compBuffer_;
    };
}

#endif
