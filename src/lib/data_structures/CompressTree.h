#ifndef LIB_COMPRESS_COMPRESSTREE_H
#define LIB_COMPRESS_COMPRESSTREE_H

#include <deque>
#include <pthread.h>
#include <queue>
#include "CTNode.h"
#include "Accumulator.h"
#include "PartialAgg.h"

namespace compresstree {

//    const size_t BUFFER_SIZE = 256;
//    const size_t BUFFER_SIZE = 10485760;
//    const size_t BUFFER_SIZE = 20971520;
    const size_t BUFFER_SIZE = 31457280;
    const size_t EMPTY_THRESHOLD = BUFFER_SIZE / 2;
    const size_t MAX_ELS_PER_BUFFER = BUFFER_SIZE / 16;

    enum CompressAlgorithm {
        SNAPPY,
        ZLIB
    };
    
    class Node;

    class CompressTree :
            public Accumulator
    {
        static uint32_t nodeCtr;
        friend class Node;
      public:
        CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                size_t (*createPAOFunc)(Token* t, PartialAgg** p),
                void (*destroyPAOFunc)(PartialAgg* p));
        ~CompressTree();

        /* Insert record into tree */
        bool insert(void* hash, PartialAgg* agg, PartialAgg**& evicted, 
                size_t& num_evicted);
        /* read values */
        bool nextValue(void*& hash, PartialAgg*& agg);
        /* get a,b values of(a,b)-tree... */
        void getAB(uint32_t& a, uint32_t& b);
      private:
        /* Add leaf whose buffer is full to be emptied once all internal node
         * buffers have been emptied */
        bool addLeafToEmpty(Node* node);
        bool asyncSignal();
        void* callCompress();
        static void* callCompressHelper(void *context);
        bool createNewRoot(Node* otherChild);
        void emptyTree();
        /* Write out all buffers to leaves. Do this before reading */
        bool flushBuffers();
        size_t handleFullLeaves();

      private:
        // (a,b)-tree...
        const uint32_t a_;
        const uint32_t b_;
        size_t (*createPAO_)(Token* t, PartialAgg** p);
        void (*destroyPAO_)(PartialAgg* p);
        CompressAlgorithm alg_;
        Node* rootNode_;
        bool allFlushed_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        size_t lastLeafRead_;
        size_t lastOffset_;
        char* serBuf_;          // buffer used for serializing PAOs

        /* Compression-related */
        pthread_t compressionThread_;
        pthread_cond_t nodesReadyForCompression_;
        pthread_mutex_t readyNodesMutex_;
        std::deque<Node*> nodesToCompress_;

        /* Eviction-related */
        uint32_t nodesInMemory_;
        size_t numEvicted_;
        char* evictedBuffer_;

        /* Node related */
        uint64_t** els_;         // pointers to elements in buffer
        char* auxBuffer_;       // used in sorting
        char* compBuffer_;
    };
}

#endif
