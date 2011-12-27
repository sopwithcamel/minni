#include <stdio.h>
#include <stdint.h>

#include "CompressTree.h"

namespace compresstree {

    CompressTree::CompressTree(uint32_t a, uint32_t b) :
        a_(a),
        b_(b),
        lastLeafRead_(0),
        lastOffset_(0)
    {
        // create root node; initially a leaf
        rootNode_ = new Node(LEAF, this);
    }

    CompressTree::~CompressTree()
    {
        delete rootNode_;
    }

    bool CompressTree::insert(uint64_t hash, void* buf, size_t buf_size)
    {
        // copy buf into root node buffer
        // root node buffer always decompressed
        return rootNode_->insert(hash, buf, buf_size);
    }

    bool CompressTree::flushBuffers()
    {
        Node* curNode;
        std::queue<Node*> visitQueue;
        visitQueue.push(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop();
            // flush buffer
            curNode->emptyBuffer();
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                if (!curNode->children_[i]->isLeaf())
                    visitQueue.push(curNode->children_[i]);
                else
                    allLeaves_.push_back(curNode->children_[i]);
            }
        }
        fprintf(stderr, "Tree has %d leaves\n", allLeaves_.size());
        return true;
    }

    bool CompressTree::nextValue(uint64_t& hash, char*& buf, size_t& buf_size)
    {
        Node* curLeaf = allLeaves_[lastLeafRead_];

        hash = *(uint64_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(uint64_t);
        buf_size = *(size_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(size_t);
        buf = curLeaf->data_ + lastOffset_;
        lastOffset_ += buf_size;
        if (lastOffset_ >= BUFFER_SIZE) {
            if (++lastLeafRead_ == allLeaves_.size())
                return false;
            lastOffset_ = 0;
        }
        return true;
    }

    bool CompressTree::addLeafToEmpty(Node* node)
    {
        leavesToBeEmptied_.push(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    bool CompressTree::handleFullLeaves()
    {
        for (uint32_t i=0; i<leavesToBeEmptied_.size(); i++) {
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

    bool CompressTree::createNewRoot(uint64_t med, Node* otherChild)
    {
        Node* newRoot = new Node(NON_LEAF, this);
        // add two children of new root
        newRoot->addChild(med, rootNode_);
        newRoot->addChild(UINT64_MAX, otherChild);
        rootNode_ = newRoot;
        return true;
    }
}
