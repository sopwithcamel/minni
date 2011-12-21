#include "CompressTree.h"

namespace compresstree {

    CompressTree::CompressTree()
    {
        // create root node
        rootNode_ = new Node(ROOT);
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
        nodesToBeEmptied_.push(node);
    }
}
