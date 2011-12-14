#ifndef LIB_BT_NODE_H
#define LIB_BT_NODE_H
#include <iostream>
#include <string>
#include <vector>

#include "Block.h"

namespace buffertree {

    enum NodeType {
        ROOT,
        INTERNAL,
        LEAF
    };

    class Node {
      public:
        /* add a block into the node buffer. If the block causes the buffer
         * to overflow, emptyBuffer() is called. */
        bool addBlock(const Block& block);
        /* add a child to this node. If the number of children becomes > b, 
         * the node is split and the children shared. */
        bool addChild(Node& node);
        /* Write out all buffers to leaves. Do this before reading */
        bool emptyAllBuffers();
        // am i a leaf node?
        bool isLeafNode();
        // am i an internal node?
        bool isInternalNode();
        /* Check if the buffer is loaded in memory */
        bool isInMemory();
      private:
        /* empty the buffer into the buffers in the next level. If any buffer
         * in the next level overflows, recursively call emptyBuffer(). Before
         * writing, the buffer is read into memory, sorted and aggregated and
         * records are written out in the form of blocks into children */
        bool emptyBuffer();
        bool splitNode();

        /* Load buffer into memory */
        bool loadBuffer();

      private:
        /* File acting as buffer for node */
        std::string bufferFile_;
        NodeType typ_;
        /* Buffer pointer if loaded in memory */
        char* data_;
        /* the number of blocks that have been written */
        size_t numBlocks_;
        /* Flag denoting whether the buffer is loaded in memory */
        bool isInMemory;

        /* Pointers to children */
        std::vector<Node*> children;
    };
}

#endif
