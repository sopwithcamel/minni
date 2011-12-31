#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

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

        // aux buffer for use in sorting
        auxBuffer_ = (char*)malloc(BUFFER_SIZE);
    }

    CompressTree::~CompressTree()
    {
        delete rootNode_;
        free(auxBuffer_);
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
begin_flush:
        visitQueue.push(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop();
            // flush buffer
            curNode->sortBuffer();
            curNode->emptyBuffer();
            if (handleFullLeaves() > 0) {
                while (!visitQueue.empty())
                    visitQueue.pop();
                goto begin_flush;
            }
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                if (!curNode->children_[i]->isLeaf()) {
                    visitQueue.push(curNode->children_[i]);
                }
            }
        }

        // add all leaves
        visitQueue.push(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop();
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                if (!curNode->children_[i]->isLeaf()) {
                    visitQueue.push(curNode->children_[i]);
                } else {
                    allLeaves_.push_back(curNode->children_[i]);
                }
            }
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Tree has %ld leaves\n", allLeaves_.size());
#endif
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
        if (lastOffset_ >= curLeaf->curOffset_) {
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
    size_t CompressTree::handleFullLeaves()
    {
        size_t num = leavesToBeEmptied_.size();
        for (uint32_t i=0; i<leavesToBeEmptied_.size(); i++) {
            Node* node = leavesToBeEmptied_.front();
            leavesToBeEmptied_.pop();
            node->decompress();
            node->sortBuffer();
            while (node->isFull()) {
                assert(node->splitLeaf());
            }
            node->compress();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n", node->id_);
#endif
        }
        return num;
    }

    bool CompressTree::createNewRoot(uint64_t med, Node* otherChild)
    {
        Node* newRoot = new Node(NON_LEAF, this);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d is new root; children are %d and %d\n", 
                newRoot->id_, rootNode_->id_, otherChild->id_);
#endif
        // add two children of new root
        newRoot->addChild(med, rootNode_, LEFT);
        newRoot->addChild(UINT64_MAX, otherChild, LEFT);
        rootNode_ = newRoot;
        return true;
    }
}
