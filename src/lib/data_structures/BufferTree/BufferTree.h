#ifndef LIB_BUFFERTREE_H
#define LIB_BUFFERTREE_H

namespace buffertree {

    const size_t BLOCKS_PER_BUFFER = 4096;
    const size_t BLOCKS_THRESHOLD = 2048;
    const size_t BLOCK_SIZE = 4096;
    

    class BufferTree {
      public:
        BufferTree();
        ~BufferTree();

        /* Insert record into Block to sort */
        bool insert(uint64_t hash, void* buf, size_t buf_size);
      public:
        // (a,b)-tree...
        const uint32_t a;
        const uint32_t b;
      private:
        Node* rootNode_;
        Block* curBlock;
        
    };
}

#endif
