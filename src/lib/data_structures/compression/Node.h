#ifndef LIB_COMPRESS_NODE_H
#define LIB_COMPRESS_NODE_H
#include <iostream>
#include <string>
#include <vector>

#include "Block.h"

namespace compresstree {

    enum NodeType {
        ROOT,
        INTERNAL,       // any node that is not a leaf or the root
        LEAF            // a leaf node is an actual leaf
    };

    class Node {
        friend class CompressTree;
      public:
        /* copy user data into buffer. Buffer should be decompressed
           before calling. */
        bool insert(uint64_t hash, void* buf, size_t buf_size);
        /* copy entire buffer into the this node's buffer. This can be used
         * by the parent to write a set of sorted elements directly into the
         * child.
         */
        bool copyIntoBuffer(void* buf, size_t buf_size);
        /* add a child to this node. If the number of children becomes > b, 
         * the node is split and the children shared. */
        bool addChild(Node& node);
        /* Write out all buffers to leaves. Do this before reading */
        bool emptyAllBuffers();

        // identification functions
        bool isLeaf();
        bool isInternal();
        bool isRoot();

        bool isFull();

        bool compress();
        bool decompress();
        bool isCompressed();
      private:
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
        /* Full leaves are dealt with by redistribution among leaves or 
         * creation of more */
        bool handleFullLeaves();
        /* sort the buffer based on hash value */
        bool sortBuffer();
        /* called when the node has to be split. Must be called with the buffer
         * decompressed and sorted */
        bool splitNode();

      private:
        /* pointer to the tree */
        CompressTree* tree;
        NodeType typ_;
        /* Buffer pointer */
        char* data_;
        size_t numElements_;
        size_t curOffset_;
        bool isCompressed;

        /* Pointers to children */
        std::vector<Node*> children;
        /* Separation values for children */
        std::vector<uint64_t> sepValues_;
    };
}

#endif
