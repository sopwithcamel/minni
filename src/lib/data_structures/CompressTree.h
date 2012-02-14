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

    class CompressTree :
            public Accumulator
    {
        static uint32_t nodeCtr;
        static uint64_t actr, bctr, cctr;
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
      private:
        /* Add leaf whose buffer is full to be emptied once all internal node
         * buffers have been emptied */
        void addNodeToSort(Node* node);
        bool wakeupSorter();
        void* callSort();
        static void* callSortHelper(void* context);
        bool addLeafToEmpty(Node* node);
        bool wakeupEmptier();
        /* Add a node to the list of nodes whose buffers have to be emptied */
        void addNodeToEmpty(Node* node);
        void* callEmpty();
        static void* callEmptyHelper(void* context);
        void* callCompress();
        static void* callCompressHelper(void *context);
        bool wakeupCompressor();
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
        size_t (*createPAO_)(Token* t, PartialAgg** p);
        void (*destroyPAO_)(PartialAgg* p);
        CompressAlgorithm alg_;
        Node* rootNode_;
        bool allFlush_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        size_t lastLeafRead_;
        size_t lastOffset_;
        char* serBuf_;          // buffer used for serializing PAOs
        bool threadsStarted_;
        bool inputComplete_;
        pthread_barrier_t threadsBarrier_;

        /* Compression-related */
        pthread_t compressionThread_;
        pthread_cond_t nodesReadyForCompression_;
        pthread_mutex_t nodesReadyForCompressMutex_;
        pthread_cond_t compressionDone_;
        pthread_mutex_t compressionDoneMutex_;
        bool askForCompressionDoneNotice_;
        std::deque<Node*> nodesToCompress_;

        /* Eviction-related */
        uint32_t nodesInMemory_;
        size_t numEvicted_;
        char* evictedBuffer_;
        size_t evictedBufferOffset_;
        pthread_mutex_t evictedBufferMutex_;

        /* Node related */
        char* auxBuffer_;       // used in sorting
        char* compBuffer_;

        /* Members for async-emptying */
        pthread_t emptierThread_;
        pthread_mutex_t rootNodeAvailableMutex_;
        pthread_cond_t rootNodeAvailableForWriting_;
        pthread_cond_t nodesReadyForEmptying_;
        pthread_mutex_t nodesReadyForEmptyMutex_;
        pthread_cond_t emptyingDone_;
        pthread_mutex_t emptyingDoneMutex_;
        bool askForEmptyingDoneNotice_;
        std::deque<Node*> nodesToEmpty_;

        /* Members for async-sorting */
        pthread_t sorterThread_;
        pthread_cond_t nodesReadyForSorting_;
        pthread_mutex_t nodesToSortMutex_;
        std::deque<Node*> nodesToSort_;
    };
}

#endif
