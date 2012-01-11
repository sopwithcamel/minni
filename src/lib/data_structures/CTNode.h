#ifndef LIB_COMPRESS_NODE_H
#define LIB_COMPRESS_NODE_H
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "CompressTree.h"
#include "PartialAgg.h"

#define ENABLE_ASSERT_CHECKS
//#define CT_NODE_DEBUG
//#define ENABLE_SORT_VERIFICATION
#define ENABLE_INTEGRITY_CHECK
#define ENABLE_COMPRESSION

#define CALL_MEM_FUNC(object,ptrToMember) ((object).*(ptrToMember))

namespace compresstree {

    class CompressTree;

    enum NodeType {
        NON_LEAF,       // any node that is not a leaf or the root
        LEAF            // a leaf node is an actual leaf
    };

    enum ChildType {
        LEFT,
        RIGHT
    };

    class Node {
        friend class CompressTree;
        typedef bool (Node::*NodeCompFn)();
      public:
        Node(NodeType typ, CompressTree* tree);
        ~Node();
        /* copy user data into buffer. Buffer should be decompressed
           before calling. */
        bool insert(uint64_t hash, void* buf, size_t buf_size);

        // identification functions
        bool isLeaf();
        bool isInternal();
        bool isRoot();

        bool isFull();
      private:

        /* Buffer handling functions */

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
        /* sort the buffer based on hash value. After sorting perform an
         * aggregation pass.
         * Must be called when buffer is decompressed */
        bool sortBuffer();
        /* advance n elements in the buffer; the starting offset must be
         * passed, which is updated; returns false if past end */
        bool advance(size_t n, size_t& offset);
        void addElements(size_t numElements);
        void reduceElements(size_t numElements);
        /* copy entire buffer into the this node's buffer. This can be used
         * by the parent to write a set of sorted elements directly into the
         * child.
         */
        bool copyIntoBuffer(void* buf, size_t buf_size);
        /* get pointer to the value stored given the pointer to the hash */
        char* getValue(uint64_t* hashPtr);
        /* converts a pointer to a hash-value to a deserialized pao */
        void deserializePAO(uint64_t* hashPtr, PartialAgg*& pao);

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
        void quicksort(uint64_t** arr, size_t left, size_t right);
        size_t partition(uint64_t** arr, size_t left, size_t right,
                size_t pivIndex);
        void swapPointers(uint64_t*& el1, uint64_t*& el2);
        void verifySort();
        void setSeparator(uint64_t sep);

        /* Compression-related functions */
        NodeCompFn compress;
        NodeCompFn decompress;
        bool asyncCompress();
        bool snappyCompress();
        bool snappyDecompress();
        bool zlibCompress();
        bool zlibDecompress();
        bool isCompressed();
        void setCompressible(bool flag);

      private:
        /* pointer to the tree */
        CompressTree* tree_;
        NodeType typ_;
        /* Buffer pointer */
        char* data_;
        uint32_t id_;
        Node* parent_;
        size_t numElements_;
        size_t curOffset_;

        /* Pointers to children */
        std::vector<Node*> children_;
        uint64_t separator_;

        /* Compression related */
        bool isCompressed_;
        bool compressible_;
        size_t compLength_;
    };
}

#endif
