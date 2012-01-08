#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "BufferTree.h"

namespace buffertree {

    BufferTree::BufferTree(uint32_t a, uint32_t b, 
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p)) :
        a_(a),
        b_(b),
        createPAO_(createPAOFunc),
        destroyPAO_(destroyPAOFunc),
        allFlushed_(false),
        lastLeafRead_(0),
        lastOffset_(0)
    {
        // create root node; initially a leaf
        rootNode_ = new Node(LEAF, this);
        rootNode_->setSeparator(UINT64_MAX);
        rootNode_->setFlushable(false);

        serBuf_ = (char*)malloc(10240);

        // aux buffer for use in sorting
        auxBuffer_ = (char*)malloc(BUFFER_SIZE);

        // allocate space for element pointers
        els_ = (uint64_t**)malloc(sizeof(uint64_t*) * MAX_ELS_PER_BUFFER);
    }

    BufferTree::~BufferTree()
    {
        delete rootNode_;
        free(serBuf_);
        free(auxBuffer_);
        free(els_);
    }

    bool BufferTree::insert(void* hash, PartialAgg* agg)
    {
        // copy buf into root node buffer
        // root node buffer always in memory
        allFlushed_ = false;
        agg->serialize(serBuf_);
        return rootNode_->insert(*(uint64_t*)hash, serBuf_, strlen(serBuf_));
    }

    bool BufferTree::nextValue(void*& hash, PartialAgg*& agg)
    {
        if (!allFlushed_)
            flushBuffers();
        Node* curLeaf = allLeaves_[lastLeafRead_];
        if (curLeaf->isFlushed())
            curLeaf->load();

        hash = (uint64_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(uint64_t);
        size_t buf_size = *(size_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(size_t);
        char* buf = curLeaf->data_ + lastOffset_;
        lastOffset_ += buf_size;
        agg->deserialize(buf);
        if (lastOffset_ >= curLeaf->curOffset_) {
            curLeaf->flush();
            if (++lastLeafRead_ == allLeaves_.size())
                return false;
            allLeaves_[lastLeafRead_]->load();
            lastOffset_ = 0;
        }
        return true;
    }

    bool BufferTree::flushBuffers()
    {
        Node* curNode;
        std::queue<Node*> visitQueue;
begin_flush:
        visitQueue.push(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop();
            // flush buffer
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
            if (curNode->isLeaf())
                allLeaves_.push_back(curNode);
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
        size_t numit = 0;
        for (int i=0; i<allLeaves_.size(); i++)
            numit += allLeaves_[i]->numElements_;
        fprintf(stderr, "Tree has %ld elements\n", numit);
#endif
        allFlushed_ = true;
        return true;
    }

    void BufferTree::getAB(uint32_t& a, uint32_t& b)
    {
        a = a_;
        b = b_;
    }

    bool BufferTree::addLeafToEmpty(Node* node)
    {
        leavesToBeEmptied_.push(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    size_t BufferTree::handleFullLeaves()
    {
        size_t num = leavesToBeEmptied_.size();
        for (uint32_t i=0; i<leavesToBeEmptied_.size(); i++) {
            Node* node = leavesToBeEmptied_.front();
            leavesToBeEmptied_.pop();
            if (node->isFlushed()) {
                node->load();
            }
            node->sortBuffer();
            Node* newLeaf = node->splitLeaf();
            if (node->isFull()) {
                Node* l1 = node->splitLeaf();
                l1->flush();
            }
            if (newLeaf->isFull()) {
                Node* l2 = newLeaf->splitLeaf();
                l2->flush();
            }
            node->flush();
            newLeaf->flush();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n", node->id_);
#endif
        }
        return num;
    }

    bool BufferTree::createNewRoot(Node* otherChild)
    {
        Node* newRoot = new Node(NON_LEAF, this);
        newRoot->setSeparator(UINT64_MAX);
        newRoot->setFlushable(false);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d is new root; children are %d and %d\n", 
                newRoot->id_, rootNode_->id_, otherChild->id_);
#endif
        // add two children of new root
        newRoot->addChild(rootNode_);
        newRoot->addChild(otherChild);
        rootNode_ = newRoot;
        return true;
    }
}
