#ifndef LIB_COMPRESS_COMPRESSTREE_H
#define LIB_COMPRESS_COMPRESSTREE_H

#include <deque>
#include <pthread.h>
#include <queue>
#include "CTConfig.h"
#include "CTNode.h"
#include "Accumulator.h"
#include "PartialAgg.h"

namespace compresstree {

    extern uint32_t BUFFER_SIZE;
    extern uint32_t MAX_ELS_PER_BUFFER;
    extern uint32_t EMPTY_THRESHOLD;

    enum CompressAlgorithm {
        SNAPPY,
        ZLIB
    };

    enum EmptyType {
        ALWAYS,
        IF_FULL
    };
    
    class Node;
    class Emptier;
    class Compressor;
    class Sorter;
    class Pager;
#ifdef ENABLE_COUNTERS
    class Monitor;
#endif

    class CompressTree :
            public Accumulator
    {
        friend class Node;
        friend class Compressor;
        friend class Emptier;
        friend class Sorter;
        friend class Pager;
#ifdef ENABLE_COUNTERS
        friend class Monitor;
#endif
      public:
        CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                uint32_t buffer_size, uint32_t pao_size,
                size_t (*createPAOFunc)(Token* t, PartialAgg** p),
                void (*destroyPAOFunc)(PartialAgg* p));
        ~CompressTree();

        /* Insert record into tree */
        bool insert(void* hash, PartialAgg* agg, PartialAgg**& evicted, 
                size_t& num_evicted, size_t max_evictable);
        /* read values */
        bool nextValue(void*& hash, PartialAgg*& agg);
      private:
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
        Node* inputNode_;
        bool allFlush_;
        EmptyType emptyType_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        uint32_t lastLeafRead_;
        uint32_t lastOffset_;
        uint32_t lastElement_;

        /* Slave-threads */
        bool threadsStarted_;
        pthread_barrier_t threadsBarrier_;

        /* Eviction-related */
        uint32_t nodesInMemory_;
        uint32_t numEvicted_;
        char* evictedBuffer_;
        uint32_t evictedBufferOffset_;
        pthread_mutex_t evictedBufferMutex_;

        /* Members for async-emptying */
        Emptier* emptier_;
        pthread_mutex_t rootNodeAvailableMutex_;
        pthread_cond_t rootNodeAvailableForWriting_;

        /* Members for async-sorting */
        Sorter* sorter_;
        
        /* Compression-related */
        Compressor* compressor_;

#ifdef ENABLE_PAGING
        /* Paging */
        Pager* pager_;
#endif

#ifdef ENABLE_COUNTERS
        /* Monitor */
        Monitor* monitor_;
#endif
    };
}

#endif
