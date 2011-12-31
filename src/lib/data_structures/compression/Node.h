#ifndef LIB_COMPRESS_NODE_H
#define LIB_COMPRESS_NODE_H
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "CompressTree.h"

#define CT_NODE_DEBUG

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

        bool compress();
        bool decompress();
        bool isCompressed();
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
         * aggregation pass */
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

        /* Tree-related functions */

        /* split leaf node */
        bool splitLeaf();
        /* Add a new child to the node; the child type indicates which side 
         * of the separator the child must be inserted. 
         * if the number of children is more than the allowed number:
         * + first check if siblings have fewer children
         * + if not, split the node into two and call addChild recursively
         */
        bool addChild(uint64_t med, Node* newNode, ChildType ctyp);
        /* Split non-leaf node; must be called with the buffer decompressed
         * and sorted. If called on the root, then a new root is created */
        bool splitNonLeaf();

        /* Sorting-related functions */
        void quicksort(uint64_t** arr, size_t left, size_t right);
        size_t partition(uint64_t** arr, size_t left, size_t right,
                size_t pivIndex);
        void swapPointers(uint64_t*& el1, uint64_t*& el2);

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
        bool isCompressed_;

        /* Pointers to children */
        std::vector<Node*> children_;
        /* Separation values for children */
        std::vector<uint64_t> sepValues_;

        /* Sorting related */
        uint64_t** els_;         // pointers to elements in buffer
    };
}

#endif
