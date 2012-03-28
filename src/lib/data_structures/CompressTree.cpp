#include <assert.h>
#include <deque>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "Buffer.h"
#include "CompressTree.h"
#include "Slaves.h"

namespace compresstree {

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
        lastElement_(0),
        threadsStarted_(false),
        nodesInMemory_(nodesInMemory),
        numEvicted_(0)
    {
        pthread_mutex_init(&rootNodeAvailableMutex_, NULL);
        pthread_cond_init(&rootNodeAvailableForWriting_, NULL);

#ifdef ENABLE_EVICTION
        pthread_mutex_init(&evictedBufferMutex_, NULL);
#endif
        uint32_t threadCount = 4;
#ifdef ENABLE_PAGING
        threadCount++;
#endif
#ifdef ENABLE_COUNTERS
        threadCount++;
        monitor_ = NULL;
#endif
        pthread_barrier_init(&threadsBarrier_, NULL, threadCount);
    }

    CompressTree::~CompressTree()
    {
        pthread_cond_destroy(&rootNodeAvailableForWriting_);
        pthread_mutex_destroy(&rootNodeAvailableMutex_);
#ifdef ENABLE_EVICTION
        pthread_mutex_destroy(&evictedBufferMutex_);
#endif
        pthread_barrier_destroy(&threadsBarrier_);
    }

    bool CompressTree::insert(void* hash, PartialAgg* agg, PartialAgg**& evicted,
            size_t& num_evicted, size_t max_evictable)
    {
        // copy buf into root node buffer
        // root node buffer always decompressed
        allFlush_ = false;
        if (!threadsStarted_) {
            startThreads();
        }
        if (inputNode_->isFull()) {
            // check if rootNode_ is available
            pthread_mutex_lock(&rootNodeAvailableMutex_);
            while (!rootNode_->buffer_.empty() || 
                    rootNode_->queuedForEmptying_) {
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "inserter sleeping\n");
#endif
                pthread_cond_wait(&rootNodeAvailableForWriting_,
                        &rootNodeAvailableMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "inserter fingered\n");
#endif
            }
            // switch buffers
            Buffer temp = rootNode_->buffer_;
            rootNode_->buffer_ = inputNode_->buffer_;
            inputNode_->buffer_ = temp;
            temp.clear();

            pthread_mutex_unlock(&rootNodeAvailableMutex_);

            // schedule the root node for emptying
            sorter_->addNode(rootNode_);
            sorter_->wakeup();
        }
        std::string serialized;
        ((ProtobufPartialAgg*)agg)->serialize(&serialized);
        bool ret = inputNode_->insert(*(uint64_t*)hash, serialized);

#ifdef ENABLE_EVICTION
        // check if any elements were evicted and pick those up
        // returns non-zero if unsucc.
        if (pthread_mutex_trylock(&evictedBufferMutex_)) {
            num_evicted = 0;
            return ret; 
        }
        num_evicted = numEvicted_;
        if (numEvicted_ > 0) {
            char* buf = evictedBuffer_;
            uint32_t offset = 0;
            uint32_t* bufSize;
            fprintf(stderr, "Evicting %lu elements\n", numEvicted_);
            PartialAgg* p;
            for (uint32_t i=0; i<numEvicted_; i++) {
                createPAO_(NULL, &p);
                rootNode_->deserializePAO((uint32_t*)(buf + offset), p);
                evicted[i] = p;
                offset += sizeof(uint32_t);
                bufSize = (uint32_t*)(buf + offset);
                offset += sizeof(uint32_t) + *bufSize;
            }
            numEvicted_ = 0;
            evictedBufferOffset_ = 0;
        }
        pthread_mutex_unlock(&evictedBufferMutex_);
#endif
        return ret;
    }

    bool CompressTree::nextValue(void*& hash, PartialAgg*& agg)
    {
        if (!allFlush_) {
            /* wait for all nodes to be sorted and emptied
               before proceeding */
/*
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
*/
            flushBuffers();

            /* Wait for all outstanding compression work to finish */
            compressor_->waitUntilCompletionNoticeReceived();
            allFlush_ = true;

            // page in and decompress first leaf
            Node* curLeaf = allLeaves_[0];
#ifdef ENABLE_PAGING
            if (curLeaf->isPagedOut()) {
                pager_->pageIn(curLeaf);
                curLeaf->waitForPageIn();
            }
#endif
            CALL_MEM_FUNC(*curLeaf, curLeaf->decompress)();
            curLeaf->waitForCompressAction(Node::DECOMPRESS);
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        Buffer::List* l = curLeaf->buffer_.lists_[0];
        hash = (void*)&l->hashes_[lastElement_];
        createPAO_(NULL, &agg);
        if (!((ProtobufPartialAgg*)agg)->deserialize(l->data_ + lastOffset_,
                l->sizes_[lastElement_])) {
            fprintf(stderr, "Node: %d; num lists: %d\n", curLeaf->id_, curLeaf->buffer_.lists_.size());
            assert(false);
        }
        lastOffset_ += l->sizes_[lastElement_];
        lastElement_++;

        if (lastElement_ >= curLeaf->buffer_.numElements()) {
            CALL_MEM_FUNC(*curLeaf, curLeaf->compress)();
            if (++lastLeafRead_ == allLeaves_.size()) {
                /* Wait for all outstanding compression work to finish */
                compressor_->waitUntilCompletionNoticeReceived();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Emptying tree!\n");
#endif
                emptyTree();
                stopThreads();
                return false;
            }
            Node *n = allLeaves_[lastLeafRead_];
#ifdef ENABLE_PAGING
            pager_->pageIn(n);
            n->waitForPageIn();
#endif
            CALL_MEM_FUNC(*n, n->decompress)();
            n->waitForCompressAction(Node::DECOMPRESS);
            lastOffset_ = 0;
            lastElement_ = 0;
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
            for (uint32_t i=0; i<n->children_.size(); i++) {
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
        lastElement_ = 0;
    
        nodeCtr = 0;
    }

    bool CompressTree::flushBuffers()
    {
        Node* curNode;
        std::deque<Node*> visitQueue;
        fprintf(stderr, "Starting to flush\n");
        // check if rootNode_ is available
        pthread_mutex_lock(&rootNodeAvailableMutex_);
        while (rootNode_->isFull()) {
            pthread_cond_wait(&rootNodeAvailableForWriting_,
                    &rootNodeAvailableMutex_);
        }
        pthread_mutex_unlock(&rootNodeAvailableMutex_);
        // root node is now empty
        emptyType_ = ALWAYS;

        // switch buffers
        Buffer temp = rootNode_->buffer_;
        rootNode_->buffer_ = inputNode_->buffer_;
        inputNode_->buffer_ = temp;
        temp.clear();

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
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Pushing node %d to all-leaves\t", curNode->id_);
                fprintf(stderr, "Now has: ");
                for (int i=0; i<allLeaves_.size(); i++) {
                    fprintf(stderr, "%d ", allLeaves_[i]->id_);
                }
                fprintf(stderr, "\n");
#endif
                continue;
            }
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                visitQueue.push_back(curNode->children_[i]);
            }
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Tree has %ld leaves\n", allLeaves_.size());
        uint32_t numit = 0;
        for (int i=0; i<allLeaves_.size(); i++)
            numit += allLeaves_[i]->buffer_.numElements();
        fprintf(stderr, "Tree has %ld elements\n", numit);
#endif
        return true;
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
            pthread_mutex_lock(&node->queuedForEmptyMutex_);
            node->queuedForEmptying_ = false;
            pthread_mutex_unlock(&node->queuedForEmptyMutex_);
        }
    }

    void CompressTree::startThreads()
    {
        // create root node; initially a leaf
        rootNode_ = new Node(this, 0);
        rootNode_->buffer_.addList();
        rootNode_->separator_ = UINT32_MAX;
        rootNode_->setCompressible(false);

        inputNode_ = new Node(this, 0);
        inputNode_->buffer_.addList();
        inputNode_->separator_ = UINT32_MAX;
        inputNode_->setCompressible(false);
        
#ifdef ENABLE_EVICTION
        // buffer for holding evicted values
        evictedBuffer_ = (char*)malloc(BUFFER_SIZE);
#endif
        emptyType_ = IF_FULL;

        pthread_attr_t attr;

        sorter_ = new Sorter(this);
        pthread_attr_init(&attr);
        pthread_create(&sorter_->thread_, &attr,
                &Sorter::callHelper, (void*)sorter_);

        compressor_ = new Compressor(this);
        pthread_attr_init(&attr);
        pthread_create(&compressor_->thread_, &attr, 
                &Compressor::callHelper, (void*)compressor_);

        emptier_ = new Emptier(this);
        pthread_attr_init(&attr);
        pthread_create(&emptier_->thread_, &attr,
                &Emptier::callHelper, (void*)emptier_);

#ifdef ENABLE_PAGING
        pager_ = new Pager(this);
        pthread_attr_init(&attr);
        pthread_create(&pager_->thread_, &attr,
                &Pager::callHelper, (void*)pager_);
#endif

#ifdef ENABLE_COUNTERS
        monitor_ = new Monitor(this);
        pthread_attr_init(&attr);
        pthread_create(&monitor_->thread_, &attr,
                &Monitor::callHelper, (void*)monitor_);
#endif

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::stopThreads()
    {
        void* status;
        delete inputNode_;

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
#ifdef ENABLE_COUNTERS
        monitor_->setInputComplete(true);
        pthread_join(monitor_->thread_, &status);
#endif
        threadsStarted_ = false;
    }

    bool CompressTree::createNewRoot(Node* otherChild)
    {
        Node* newRoot = new Node(this, rootNode_->level() + 1);
        newRoot->buffer_.addList();
        newRoot->separator_ = UINT32_MAX;
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
