#include "CompressTree.h"

namespace compresstree {

    CompressTree::CompressTree(uint32_t a, uint32_t b) :
        a_(a),
        b_(b)
    {
        // create root node; initially a leaf
        rootNode_ = new Node(LEAF);
    }

    CompressTree::~CompressTree()
    {
        delete rootNode_;
    }

    bool CompressTree::insert(uint64_t hash, void* buf, size_t buf_size)
    {
        // copy buf into root node buffer
        // root node buffer always decompressed
        rootNode_->insert(hash, buf, buf_size);
    }


    bool CompressTree::addLeafToEmpty(Node* node)
    {
        leavesToBeEmptied_.push(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    bool CompressTree::handleFullLeaves()
    {
        for (int i=0; i<leavesToBeEmptied_.size(); i++) {
            Node* node = leavesToBeEmptied_.front();
            leavesToBeEmptied_.pop();
            node->decompress();
            node->sortBuffer();
            if (!node->splitLeaf())
                return false;
            node->compress();
        }
        return true;
    }
}
