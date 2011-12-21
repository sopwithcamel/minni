#ifndef LIB_COMPRESSTREE_H
#define LIB_COMPRESSTREE_H

#include <queue>

namespace compresstree {

    const size_t BLOCKS_PER_BUFFER = 4096;
    const size_t EMPTY_THRESHOLD = 131072;
    const size_t BUFFER_SIZE = 262144;
    

    class CompressTree {
      public:
        CompressTree();
        ~CompressTree();

        /* Insert record into tree */
        bool insert(uint64_t hash, void* buf, size_t buf_size);
      private:
        /* Add leaf whose buffer is full to be emptied once all internal node
         * buffers have been emptied */
        void addLeafToEmpty(Node* node);
      public:
        // (a,b)-tree...
        const uint32_t a;
        const uint32_t b;
      private:
        Node* rootNode_;
        std::queue<Node*> nodesToBeEmptied_;
    };
}

#endif
