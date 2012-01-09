#ifndef LIB_BUFFERTREE_BUFFERTREE_H
#define LIB_BUFFERTREE_BUFFERTREE_H

#include <queue>
#include "BTNode.h"
#include "Accumulator.h"
#include "PartialAgg.h"

namespace buffertree {

//    const size_t BUFFER_SIZE = 10485760;
    const size_t BUFFER_SIZE = 2048;
//    const size_t BUFFER_SIZE = 629145600;
    const size_t EMPTY_THRESHOLD = BUFFER_SIZE / 2.5;
    const size_t MAX_ELS_PER_BUFFER = BUFFER_SIZE / 16;

    enum CompressAlgorithm {
        SNAPPY,
        ZLIB
    };
    
    class Node;

    class BufferTree :
            public Accumulator
    {
        friend class Node;
      public:
        BufferTree(uint32_t a, uint32_t b,
                size_t (*createPAOFunc)(Token* t, PartialAgg** p),
                void (*destroyPAOFunc)(PartialAgg* p));
        ~BufferTree();

        /* Insert record into tree */
        bool insert(void* hash, PartialAgg* agg);
        /* read values */
        bool nextValue(void*& hash, PartialAgg*& agg);
        /* get a,b values of(a,b)-tree... */
        void getAB(uint32_t& a, uint32_t& b);
      private:
        /* Add leaf whose buffer is full to be emptied once all internal node
         * buffers have been emptied */
        bool addLeafToEmpty(Node* node);
        size_t handleFullLeaves();
        bool createNewRoot(Node* otherChild);
        /* Write out all buffers to leaves. Do this before reading */
        bool flushBuffers();
      private:
        // (a,b)-tree...
        const uint32_t a_;
        const uint32_t b_;
        size_t (*createPAO_)(Token* t, PartialAgg** p);
        void (*destroyPAO_)(PartialAgg* p);
        Node* rootNode_;
        bool allFlushed_;
        std::queue<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        size_t lastLeafRead_;
        size_t lastOffset_;
        char* serBuf_;          // buffer used for serializing PAOs
        /* Node related */
        uint64_t** els_;         // pointers to elements in buffer
        char* auxBuffer_;       // used in sorting
    };
}

#endif
