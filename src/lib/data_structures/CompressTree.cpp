#include <assert.h>
#include <deque>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "CompressTree.h"

namespace compresstree {
    uint64_t CompressTree::actr = 0;
    uint64_t CompressTree::bctr = 0;
    uint64_t CompressTree::cctr = 0;

    bool CompressTree::enablePaging = false;

    CompressTree::CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                size_t (*createPAOFunc)(Token* t, PartialAgg** p),
                void (*destroyPAOFunc)(PartialAgg* p)) :
        a_(a),
        b_(b),
        nodeCtr(0),
        createPAO_(createPAOFunc),
        destroyPAO_(destroyPAOFunc),
        alg_(SNAPPY),
        allFlush_(false),
        lastLeafRead_(0),
        lastOffset_(0),
        threadsStarted_(false),
        nodesInMemory_(nodesInMemory),
        numEvicted_(0)
    {
        // create root node; initially a leaf
        rootNode_ = new Node(LEAF, this, true);
        rootNode_->setSeparator(UINT64_MAX);
        rootNode_->setCompressible(false);

        // aux buffer for use in sorting
        auxBuffer_ = (char*)malloc(BUFFER_SIZE);

        // buffer for use in compression
        compBuffer_ = (char*)malloc(BUFFER_SIZE * 2);

        // buffer for holding evicted values
        evictedBuffer_ = (char*)malloc(BUFFER_SIZE);
        Node::emptyType_ = Node::IF_FULL;
    }

    CompressTree::~CompressTree()
    {
        free(auxBuffer_);
        free(compBuffer_);
        free(evictedBuffer_);
        pthread_cond_destroy(&rootNodeAvailableForWriting_);
        pthread_mutex_destroy(&rootNodeAvailableMutex_);
        pthread_mutex_destroy(&evictedBufferMutex_);
        pthread_barrier_destroy(&threadsBarrier_);
    }

    bool CompressTree::insert(void* hash, PartialAgg* agg, PartialAgg**& evicted,
            size_t& num_evicted)
    {
        // copy buf into root node buffer
        // root node buffer always decompressed
        allFlush_ = false;
        if (!threadsStarted_) {
            startThreads();
        }
        pthread_mutex_lock(&rootNodeAvailableMutex_);
        if (rootNode_->isFull()) {
            pthread_mutex_unlock(&rootNodeAvailableMutex_);
            sorter_->addNode(rootNode_);
            sorter_->wakeup();
            pthread_mutex_lock(&rootNodeAvailableMutex_);
            while (rootNode_->isFull()) {
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "inserter sleeping\n");
#endif
                pthread_cond_wait(&rootNodeAvailableForWriting_,
                        &rootNodeAvailableMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "inserter fingered\n");
#endif
            }
        }
        std::string serialized;
        agg->serialize(&serialized);
        bool ret = rootNode_->insert(*(uint64_t*)hash, serialized);
        pthread_mutex_unlock(&rootNodeAvailableMutex_);

        // check if any elements were evicted and pick those up
        // returns non-zero if unsucc.
        if (pthread_mutex_trylock(&evictedBufferMutex_)) {
            num_evicted = 0;
            return ret; 
        }
        num_evicted = numEvicted_;
        if (numEvicted_ > 0) {
            char* buf = evictedBuffer_;
            size_t offset = 0;
            size_t* bufSize;
            fprintf(stderr, "Evicting %lu elements\n", numEvicted_);
            PartialAgg* p;
            for (uint32_t i=0; i<numEvicted_; i++) {
                createPAO_(NULL, &p);
                rootNode_->deserializePAO((uint64_t*)(buf + offset), p);
                evicted[i] = p;
                offset += sizeof(uint64_t);
                bufSize = (size_t*)(buf + offset);
                offset += sizeof(size_t) + *bufSize;
            }
            numEvicted_ = 0;
            evictedBufferOffset_ = 0;
        }
        pthread_mutex_unlock(&evictedBufferMutex_);
        return ret;
    }

    bool CompressTree::nextValue(void*& hash, PartialAgg*& agg)
    {
        if (!allFlush_) {
            /* wait for all nodes to be sorted and emptied
               before proceeding */
            do {
#ifdef ENABLE_PAGING
                pager_->waitUntilCompletionNoticeReceived();
#endif
                sorter_->waitUntilCompletionNoticeReceived();
                emptier_->waitUntilCompletionNoticeReceived();
#ifdef ENABLE_PAGING
            } while (!sorter_->empty() || !emptier_->empty() || 
                    !pager_->empty());
#else
            } while (!sorter_->empty() || !emptier_->empty());
#endif

            flushBuffers();

            /* Wait for all outstanding compression work to finish */
            compressor_->waitUntilCompletionNoticeReceived();
            allFlush_ = true;

            // page in and decompress first leaf
            Node* curLeaf = allLeaves_[0];
#ifdef ENABLE_PAGING
            if (curLeaf->isPagedOut()) {
                curLeaf->pageIn();
                curLeaf->waitForPageIn();
            }
#endif
            if (curLeaf->isCompressed()) {
                CALL_MEM_FUNC(*curLeaf, curLeaf->decompress)();
                curLeaf->waitForCompressAction();
            }
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        hash = (uint64_t*)(curLeaf->data_ + lastOffset_);
        createPAO_(NULL, &agg);
        rootNode_->deserializePAO((uint64_t*)hash, agg);
        lastOffset_ += sizeof(uint64_t);
        size_t buf_size = *(size_t*)(curLeaf->data_ + lastOffset_);
        lastOffset_ += sizeof(size_t) + buf_size;

        if (lastOffset_ >= curLeaf->curOffset_) {
            CALL_MEM_FUNC(*curLeaf, curLeaf->compress)();
            if (++lastLeafRead_ == allLeaves_.size()) {
                /* Wait for all outstanding compression work to finish */
                compressor_->waitUntilCompletionNoticeReceived();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Emptying tree!\n");
#endif
#ifdef ENABLE_COUNTERS
                fprintf(stderr, "%lu %lu %lu\n", CompressTree::actr, CompressTree::bctr, CompressTree::cctr);
#endif
                emptyTree();
                stopThreads();
                return false;
            }
            Node *n = allLeaves_[lastLeafRead_];
#ifdef ENABLE_PAGING
            n->pageIn();
            n->waitForPageIn();
#endif
            CALL_MEM_FUNC(*n, n->decompress)();
            n->waitForCompressAction();
            lastOffset_ = 0;
        }

        return true;
    }

    void CompressTree::emptyTree()
    {
        std::deque<Node*> delList1;
        std::deque<Node*> delList2;
        delList1.push_back(rootNode_);
        while (!delList1.empty()) {
            Node* n = delList1.front();
            delList1.pop_front();
            for (int i=0; i<n->children_.size(); i++) {
                delList1.push_back(n->children_[i]);
            }            
            delList2.push_back(n);
        }
        while (!delList2.empty()) {
            Node* n = delList2.front();
            delList2.pop_front();
            delete n;
        }
        allLeaves_.clear();
        leavesToBeEmptied_.clear();
        allFlush_ = false;
        lastLeafRead_ = 0;
        lastOffset_ = 0;
    
        nodeCtr = 0;
        CompressTree::actr = 0;
        CompressTree::bctr = 0;
        CompressTree::cctr = 0;
        Node::emptyType_ = Node::IF_FULL;
        rootNode_ = new Node(LEAF, this, true);
        rootNode_->setSeparator(UINT64_MAX);
        rootNode_->setCompressible(false);
    }

    bool CompressTree::flushBuffers()
    {
        Node* curNode;
        size_t prevNumSibs, newNumSibs;
        std::deque<Node*> visitQueue;
        fprintf(stderr, "Starting to flush\n");
        Node::emptyType_ = Node::ALWAYS;

        sorter_->addNode(rootNode_);
        sorter_->wakeup();

        /* wait for all nodes to be sorted and emptied
           before proceeding */
        do {
#ifdef ENABLE_PAGING
            pager_->waitUntilCompletionNoticeReceived();
#endif
            sorter_->waitUntilCompletionNoticeReceived();
            emptier_->waitUntilCompletionNoticeReceived();
#ifdef ENABLE_PAGING
        } while (!sorter_->empty() || !emptier_->empty() || !pager_->empty());
#else
        } while (!sorter_->empty() || !emptier_->empty());
#endif

        // add all leaves; 
        visitQueue.push_back(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop_front();
            if (curNode->isLeaf()) {
                allLeaves_.push_back(curNode);
                continue;
            }
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                visitQueue.push_back(curNode->children_[i]);
            }
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Tree has %ld leaves\n", allLeaves_.size());
        size_t numit = 0;
        for (int i=0; i<allLeaves_.size(); i++)
            numit += allLeaves_[i]->numElements_;
        fprintf(stderr, "Tree has %ld elements\n", numit);
#endif
        return true;
    }

    void* CompressTree::callSortHelper(void *context)
    {
        return ((CompressTree*)context)->sorter_->work();
    }

    void* CompressTree::callCompressHelper(void *context)
    {
        return ((CompressTree*)context)->compressor_->work();
    }

    void* CompressTree::callEmptyHelper(void *context)
    {
        return ((CompressTree*)context)->emptier_->work();
    }

    void* CompressTree::callPageHelper(void *context)
    {
        return ((CompressTree*)context)->pager_->work();
    }

    bool CompressTree::addLeafToEmpty(Node* node)
    {
        leavesToBeEmptied_.push_back(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    void CompressTree::handleFullLeaves()
    {
        while (!leavesToBeEmptied_.empty()) {
            Node* node = leavesToBeEmptied_.front();
            leavesToBeEmptied_.pop_front();

            node->aggregateBuffer();
            // check if sorting and aggregating made the leaf small enough
            if (!node->isFull() && node->compressible_) {
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d reduced in size, so no split\n",
                        node->id_);
#endif
                CALL_MEM_FUNC(*node, node->compress)();
                continue;
            }
            Node* newLeaf = node->splitLeaf();
            Node *l1 = NULL, *l2 = NULL;
            if (node->isFull()) {
                l1 = node->splitLeaf();
            }
            if (newLeaf && newLeaf->isFull()) {
                l2 = newLeaf->splitLeaf();
            }
            CALL_MEM_FUNC(*node, node->compress)();
            if (newLeaf)
                CALL_MEM_FUNC(*newLeaf, newLeaf->compress)();
            if (l1)
                CALL_MEM_FUNC(*l1, l1->compress)();
            if (l2)
                CALL_MEM_FUNC(*l2, l2->compress)();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n", node->id_);
#endif
        }
    }

    void CompressTree::startThreads()
    {
        pthread_mutex_init(&rootNodeAvailableMutex_, NULL);
        pthread_cond_init(&rootNodeAvailableForWriting_, NULL);

        pthread_mutex_init(&evictedBufferMutex_, NULL);
#ifdef ENABLE_PAGING
        pthread_barrier_init(&threadsBarrier_, NULL, 5);
#else
        pthread_barrier_init(&threadsBarrier_, NULL, 4);
#endif

        pthread_attr_t attr;

        sorter_ = new Sorter(this);
        pthread_attr_init(&attr);
        pthread_create(&sorter_->thread_, &attr,
                &CompressTree::callSortHelper, (void*)this);

        compressor_ = new Compressor(this);
        pthread_attr_init(&attr);
        pthread_create(&compressor_->thread_, &attr, 
                &CompressTree::callCompressHelper, (void*)this);

        emptier_ = new Emptier(this);
        pthread_attr_init(&attr);
        pthread_create(&emptier_->thread_, &attr,
                &CompressTree::callEmptyHelper, (void*)this);

#ifdef ENABLE_PAGING
        pager_ = new Pager(this);
        pthread_attr_init(&attr);
        pthread_create(&pager_->thread_, &attr,
                &CompressTree::callPageHelper, (void*)this);
#endif

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::stopThreads()
    {
        void* status;
        sorter_->setInputComplete(true);
        sorter_->wakeup();
        pthread_join(sorter_->thread_, &status);
        emptier_->setInputComplete(true);
        emptier_->wakeup();
        pthread_join(emptier_->thread_, &status);
        compressor_->setInputComplete(true);
        compressor_->wakeup();
        pthread_join(compressor_->thread_, &status);
#ifdef ENABLE_PAGING
        pager_->setInputComplete(true);
        pager_->wakeup();
        pthread_join(pager_->thread_, &status);
#endif
        threadsStarted_ = false;
    }

    bool CompressTree::createNewRoot(Node* otherChild)
    {
        Node* newRoot = new Node(NON_LEAF, this, true);
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
