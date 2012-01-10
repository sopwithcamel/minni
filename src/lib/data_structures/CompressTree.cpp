#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "CompressTree.h"

namespace compresstree {

    CompressTree::CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                size_t (*createPAOFunc)(Token* t, PartialAgg** p),
                void (*destroyPAOFunc)(PartialAgg* p)) :
        a_(a),
        b_(b),
        createPAO_(createPAOFunc),
        destroyPAO_(destroyPAOFunc),
        alg_(SNAPPY),
        allFlushed_(false),
        lastLeafRead_(0),
        lastOffset_(0),
        nodesInMemory_(nodesInMemory),
        numEvicted_(0)
    {
        // create root node; initially a leaf
        rootNode_ = new Node(LEAF, this);
        rootNode_->setSeparator(UINT64_MAX);
        rootNode_->setCompressible(false);

        // buffer for PAO serialization use
        serBuf_ = (char*)malloc(10240);

        // aux buffer for use in sorting
        auxBuffer_ = (char*)malloc(BUFFER_SIZE);

        // allocate space for element pointers
        els_ = (uint64_t**)malloc(sizeof(uint64_t*) * MAX_ELS_PER_BUFFER);

        // buffer for use in compression
        compBuffer_ = (char*)malloc(BUFFER_SIZE);

        // buffer for holding evicted values
        evictedBuffer_ = (char*)malloc(BUFFER_SIZE);
    }

    CompressTree::~CompressTree()
    {
        delete rootNode_;
        free(serBuf_);
        free(auxBuffer_);
        free(compBuffer_);
        free(evictedBuffer_);
        free(els_);
    }

    bool CompressTree::insert(void* hash, PartialAgg* agg, PartialAgg**& evicted,
            size_t& num_evicted)
    {
        // copy buf into root node buffer
        // root node buffer always decompressed
        allFlushed_ = false;
        agg->serialize(serBuf_);
        bool ret = rootNode_->insert(*(uint64_t*)hash, serBuf_, strlen(serBuf_));
        num_evicted = numEvicted_;
        if (numEvicted_ > 0) {
            char* buf = evictedBuffer_;
            size_t offset = 0;
            size_t* bufSize;
            fprintf(stderr, "Evicting %lu elements\n", numEvicted_);
            for (uint32_t i=0; i<numEvicted_; i++) {
                offset += sizeof(uint64_t);
                bufSize = (size_t*)(buf + offset);
                offset += sizeof(size_t);
                evicted[i]->deserialize(buf + offset);
                offset += *bufSize;
            }
            numEvicted_ = 0;
        }
        return ret;
    }

    bool CompressTree::nextValue(void*& hash, PartialAgg*& agg)
    {
        if (!allFlushed_)
            flushBuffers();
        Node* curLeaf = allLeaves_[lastLeafRead_];
        if (curLeaf->isCompressed())
            CALL_MEM_FUNC(*curLeaf, curLeaf->decompress)();

        hash = (uint64_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(uint64_t);
        size_t buf_size = *(size_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(size_t);
        char* buf = curLeaf->data_ + lastOffset_;
        lastOffset_ += buf_size;
        agg->deserialize(buf);
        if (lastOffset_ >= curLeaf->curOffset_) {
            CALL_MEM_FUNC(*curLeaf, curLeaf->compress)();
            if (++lastLeafRead_ == allLeaves_.size())
                return false;
            CALL_MEM_FUNC(*allLeaves_[lastLeafRead_], 
                    allLeaves_[lastLeafRead_]->decompress)();
            lastOffset_ = 0;
        }
        return true;
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

    void CompressTree::getAB(uint32_t& a, uint32_t& b)
    {
        a = a_;
        b = b_;
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
            if (node->isCompressed()) {
                CALL_MEM_FUNC(*node, node->decompress)();
            }
            node->sortBuffer();
            Node* newLeaf = node->splitLeaf();
            if (node->isFull()) {
                Node* l1 = node->splitLeaf();
                if (l1)
                    CALL_MEM_FUNC(*l1, l1->compress)();
            }
            if (newLeaf && newLeaf->isFull()) {
                Node* l2 = newLeaf->splitLeaf();
                if (l2)
                    CALL_MEM_FUNC(*l2, l2->compress)();
            }
            CALL_MEM_FUNC(*node, node->compress)();
            if (newLeaf)
                CALL_MEM_FUNC(*newLeaf, newLeaf->compress)();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n", node->id_);
#endif
        }
        return num;
    }

    bool CompressTree::createNewRoot(Node* otherChild)
    {
        Node* newRoot = new Node(NON_LEAF, this);
        newRoot->setSeparator(UINT64_MAX);
        newRoot->setCompressible(false);
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
