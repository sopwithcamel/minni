#ifndef LIB_COMPRESS_COMPRESSTREE_H
#define LIB_COMPRESS_COMPRESSTREE_H

#include <deque>
#include <pthread.h>
#include <queue>
#include "CTNode.h"
#include "Accumulator.h"
#include "PartialAgg.h"
#include "Slaves.h"

namespace compresstree {

//    const size_t BUFFER_SIZE = 256;
//    const size_t BUFFER_SIZE = 104857600;
//    const size_t BUFFER_SIZE = 20971520;
    const size_t BUFFER_SIZE = 31457280;
    const size_t EMPTY_THRESHOLD = BUFFER_SIZE / 2;
    const size_t MAX_ELS_PER_BUFFER = BUFFER_SIZE / 24;

    enum CompressAlgorithm {
        SNAPPY,
        ZLIB
    };
    
    class Node;
    class Emptier;
    class Compressor;
    class Sorter;

    class CompressTree :
            public Accumulator
    {
        static uint64_t actr, bctr, cctr;
        friend class Node;
        friend class Compressor;
        friend class Emptier;
        friend class Sorter;
        friend class Pager;
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
      private:
        static void* callSortHelper(void* context);
        static void* callEmptyHelper(void* context);
        static void* callCompressHelper(void *context);
        
        bool addLeafToEmpty(Node* node);
        bool createNewRoot(Node* otherChild);
        void emptyTree();
        /* Write out all buffers to leaves. Do this before reading */
        bool flushBuffers();
        void handleFullLeaves();
        void startThreads();
        void stopThreads();

      private:
        // (a,b)-tree...
        const uint32_t a_;
        const uint32_t b_;
        uint32_t nodeCtr;
        size_t (*createPAO_)(Token* t, PartialAgg** p);
        void (*destroyPAO_)(PartialAgg* p);
        CompressAlgorithm alg_;
        Node* rootNode_;
        bool allFlush_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        size_t lastLeafRead_;
        size_t lastOffset_;

        /* Slave-threads */
        bool threadsStarted_;
        pthread_barrier_t threadsBarrier_;

        /* Eviction-related */
        uint32_t nodesInMemory_;
        size_t numEvicted_;
        char* evictedBuffer_;
        size_t evictedBufferOffset_;
        pthread_mutex_t evictedBufferMutex_;

        /* Members for async-emptying */
        Emptier* emptier_;
        pthread_mutex_t rootNodeAvailableMutex_;
        pthread_cond_t rootNodeAvailableForWriting_;

        /* Members for async-sorting */
        Sorter* sorter_;
        char* auxBuffer_;       // used in sorting
        
        /* Compression-related */
        Compressor* compressor_;
        char* compBuffer_;
    };
}

#endif
