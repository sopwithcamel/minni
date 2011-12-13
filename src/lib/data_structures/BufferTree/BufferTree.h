#ifndef LIB_BUFFERTREE_H
#define LIB_BUFFERTREE_H

namespace buffertree {

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
