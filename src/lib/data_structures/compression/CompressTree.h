#ifndef LIB_COMPRESS_COMPRESSTREE_H
#define LIB_COMPRESS_COMPRESSTREE_H

#include <queue>
#include <stdint.h>
#include "Node.h"

namespace compresstree {

    const size_t BLOCKS_PER_BUFFER = 4096;
    const size_t BUFFER_SIZE = 2048;
    const size_t EMPTY_THRESHOLD = BUFFER_SIZE / 2;
    
    class Node;

    class CompressTree {
        friend class Node;
      public:
        CompressTree(uint32_t a, uint32_t b);
        ~CompressTree();

        /* Insert record into tree */
        bool insert(uint64_t hash, void* buf, size_t buf_size);
      private:
        /* Add leaf whose buffer is full to be emptied once all internal node
         * buffers have been emptied */
        bool addLeafToEmpty(Node* node);
        bool handleFullLeaves();
      public:
        // (a,b)-tree...
        const uint32_t a_;
        const uint32_t b_;
      private:
        Node* rootNode_;
        std::queue<Node*> leavesToBeEmptied_;
    };
}

#endif
