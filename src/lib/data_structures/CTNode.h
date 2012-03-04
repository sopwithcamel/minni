#ifndef LIB_COMPRESS_NODE_H
#define LIB_COMPRESS_NODE_H
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "Buffer.h"
#include "CompressTree.h"
#include "PartialAgg.h"

#define CALL_MEM_FUNC(object,ptrToMember) ((object).*(ptrToMember))

namespace compresstree {

    class Buffer;
    class CompressTree;
    class Emptier;
    class Compressor;
    class Sorter;

    class Node {
        friend class CompressTree;
        friend class Compressor;
        friend class Emptier;
        friend class Sorter;
        friend class Pager;
        typedef bool (Node::*NodeCompressionFn)();
        enum EmptyType {
            ALWAYS,
            IF_FULL
        };

        enum NodeState {
            DECOMPRESSED,
            COMPRESSED,
            PAGED_OUT
        };

        enum CompressionAction {
            NONE,
            COMPRESS,
            DECOMPRESS
        };

        enum PageAction {
            NO_PAGE,
            PAGE_OUT,
            PAGE_IN
        };

        class MergeComparator {
          public:
            bool operator()(uint32_t* lhs, uint32_t* rhs) const
            {
                return (*lhs < *rhs);
            } 
        };
      public:
        Node(CompressTree* tree, uint32_t level);
        ~Node();
        /* copy user data into buffer. Buffer should be decompressed
           before calling. */
        bool insert(uint64_t hash, const std::string& value);

        // identification functions
        bool isLeaf() const;
        bool isRoot() const;

        bool isFull() const;
        uint32_t level() const;
        uint32_t id() const;
      private:
        inline void setState(NodeState state);

        /* Buffer handling functions */

        bool emptyOrCompress();
        /* Function: empty the buffer into the buffers in the next level. 
         *  + Must be called with buffer decompressed.
         *  + Buffer will be freed after invocation.
         *  + If children buffers overflow, it recursively calls itself. 
         *    until the recursion reaches the leaves. At this stage, handling
         *    the leaf buffer overflows is queued for later because this may
         *    cause splitting (recursively) up the tree which is best done
         *    when no internal nodes are over-full.
         *  + an emptyBuffer() invocation should be followed by a
         *    handleFullLeaves() call.
         */
        bool emptyBuffer();
        /* Sort the root buffer based on hash value. All other nodes can
         * aggregating by merging. */
        bool sortBuffer();
        /* Aggregate the sorted root buffer */
        bool aggregateBuffer();
        /* Merge the sorted sub-lists of the buffer */
        bool mergeBuffer();
        /* copy contents from node's buffer into this buffer. Starting from
         * index = index, copy num elements' data.
         */
        bool copyIntoBuffer(Buffer::List* l, uint32_t index, uint32_t num);

        /* Tree-related functions */

        /* split leaf node and return new leaf */
        Node* splitLeaf();
        /* Add a new child to the node; the child type indicates which side 
         * of the separator the child must be inserted. 
         * if the number of children is more than the allowed number:
         * + first check if siblings have fewer children
         * + if not, split the node into two and call addChild recursively
         */
        bool addChild(Node* newNode);
        /* Split non-leaf node; must be called with the buffer decompressed
         * and sorted. If called on the root, then a new root is created */
        bool splitNonLeaf();
        bool checkIntegrity();

        /* Sorting-related functions */
        void quicksort(uint32_t left, uint32_t right);
        void waitForSort();

        /* Compression-related functions */
        NodeCompressionFn compress;
        /* Decompress node buffer. Returns true if:
         * + the buffer was decompressed successfully
         * + the node is marked as incompressible
         * + the node buffer is empty. In this case, no memory is allocated.
         * data_ may, therefore, be NULL or not and this must be checked
         * before using the node (eg. for copying data)
         */
        NodeCompressionFn decompress;
        bool asyncCompress();
        bool asyncDecompress();
        /* wait for completion of compression action on node */
        void waitForCompressAction(const CompressionAction& act);
        bool snappyCompress();
        bool snappyDecompress();
        /* Returns true if node is compressed; also true if node still hasn't
         * been paged in */
        bool isCompressed();
        void setCompressible(bool flag);

#ifdef ENABLE_PAGING
        /* Paging-related functions */
        bool waitForPageIn();
        bool pageOut();
        bool pageIn();
        bool isPagedOut();
        bool isPinned() const;
#endif

      private:
        static EmptyType emptyType_;
        /* pointer to the tree */
        CompressTree* tree_;
        /* Buffer */
        Buffer buffer_;
        NodeState state_;
        pthread_mutex_t stateMutex_;
        Buffer compressed_;
        uint32_t id_;
        /* level in the tree; 0 at leaves and increases upwards */
        uint32_t level_;
        Node* parent_;
        PartialAgg *lastPAO, *thisPAO;

        /* Pointers to children */
        std::vector<Node*> children_;
        uint32_t separator_;

        /* Emptying related */
        bool queuedForEmptying_;
        pthread_mutex_t queuedForEmptyMutex_;
        char** perm_;

        /* Compression related */
        bool compressible_;
        bool queuedForCompAct_;
        CompressionAction compAct_;
        pthread_cond_t compActCond_;
        pthread_mutex_t compActMutex_;

#ifdef ENABLE_PAGING
        /* Paging related */
        int fd_;
        bool pageable_;
        bool queuedForPaging_;
        PageAction pageAct_;
        pthread_cond_t pageCond_;
        pthread_mutex_t pageMutex_;
#endif
    };
}

#endif
