#include <assert.h>
#include <deque>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "CompressTree.h"

namespace compresstree {
    uint32_t CompressTree::nodeCtr = 0; 
    uint64_t CompressTree::actr = 0;
    uint64_t CompressTree::bctr = 0;
    uint64_t CompressTree::cctr = 0;

    CompressTree::CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                size_t (*createPAOFunc)(Token* t, PartialAgg** p),
                void (*destroyPAOFunc)(PartialAgg* p)) :
        a_(a),
        b_(b),
        createPAO_(createPAOFunc),
        destroyPAO_(destroyPAOFunc),
        alg_(SNAPPY),
        allFlush_(false),
        lastLeafRead_(0),
        lastOffset_(0),
        threadsStarted_(false),
        inputComplete_(false),
        nodesInMemory_(nodesInMemory),
        numEvicted_(0),
        askForCompressionDoneNotice_(false),
        askForEmptyingDoneNotice_(false)
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
    }

    CompressTree::~CompressTree()
    {
        free(auxBuffer_);
        free(compBuffer_);
        free(evictedBuffer_);
        pthread_cond_destroy(&nodesReadyForCompression_);
        pthread_mutex_destroy(&nodesReadyForCompressMutex_);
        pthread_cond_destroy(&compressionDone_);
        pthread_mutex_destroy(&compressionDoneMutex_);
        pthread_cond_destroy(&nodesReadyForEmptying_);
        pthread_mutex_destroy(&nodesReadyForEmptyMutex_);
        pthread_cond_destroy(&emptyingDone_);
        pthread_mutex_destroy(&emptyingDoneMutex_);
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
            addNodeToSort(rootNode_);
            wakeupSorter();
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
            /* wait for all nodes in the to-sort list to be sorted
               before proceeding */
            pthread_mutex_lock(&nodesToSortMutex_);
            while (!nodesToSort_.empty()) {
                pthread_mutex_unlock(&nodesToSortMutex_);
                usleep(10000);
                pthread_mutex_lock(&nodesToSortMutex_);
            }
            pthread_mutex_unlock(&nodesToSortMutex_);
            
            /* wait for all nodes in the to-empty list to be emptied
               before proceeding */
            pthread_mutex_lock(&nodesReadyForEmptyMutex_);
            if (!nodesToEmpty_.empty()) {
                pthread_mutex_lock(&emptyingDoneMutex_);
                askForEmptyingDoneNotice_ = true;
                pthread_mutex_unlock(&nodesReadyForEmptyMutex_);

                pthread_cond_wait(&emptyingDone_, &emptyingDoneMutex_);
                pthread_mutex_unlock(&emptyingDoneMutex_);
            } else {
                pthread_mutex_unlock(&nodesReadyForEmptyMutex_);
            }

            flushBuffers();

            /* Wait for all outstanding compression work to finish */
            pthread_mutex_lock(&nodesReadyForCompressMutex_);
            if (!nodesToCompress_.empty()) {
                pthread_mutex_lock(&compressionDoneMutex_);
                askForCompressionDoneNotice_ = true;
                pthread_mutex_unlock(&nodesReadyForCompressMutex_);

                pthread_cond_wait(&compressionDone_,
                        &compressionDoneMutex_);
                pthread_mutex_unlock(&compressionDoneMutex_);
            } else {
                pthread_mutex_unlock(&nodesReadyForCompressMutex_);
            }
            allFlush_ = true;
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        if (curLeaf->isCompressed()) {
            CALL_MEM_FUNC(*curLeaf, curLeaf->decompress)();
            curLeaf->waitForCompressAction();
        }

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
                pthread_mutex_lock(&nodesReadyForCompressMutex_);
                if (!nodesToCompress_.empty()) {
                    pthread_mutex_lock(&compressionDoneMutex_);
                    askForCompressionDoneNotice_ = true;
                    pthread_mutex_unlock(&nodesReadyForCompressMutex_);

                    pthread_cond_wait(&compressionDone_,
                            &compressionDoneMutex_);
                    pthread_mutex_unlock(&compressionDoneMutex_);
                } else {
                    pthread_mutex_unlock(&nodesReadyForCompressMutex_);
                }
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
        nodesToCompress_.clear();
        allFlush_ = false;
        lastLeafRead_ = 0;
        lastOffset_ = 0;
    
        nodeCtr = 0;
        CompressTree::actr = 0;
        CompressTree::bctr = 0;
        CompressTree::cctr = 0;
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
begin_flush:
        visitQueue.push_back(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop_front();

            // count number of siblings including itself
            prevNumSibs = curNode->getNumSiblings();

            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                CALL_MEM_FUNC(*curNode->children_[i], curNode->children_[i]->decompress)();
            }
            curNode->sortBuffer();
            curNode->aggregateBuffer();
            curNode->emptyBuffer(Node::NON_RECURSIVE);

            // count number of siblings including itself after flush
            newNumSibs = curNode->getNumSiblings();
        
            if (newNumSibs != prevNumSibs) {
                fprintf(stderr, "Node %d changed sibs %lu to %lu\n", curNode->id_, 
                        prevNumSibs, newNumSibs);
                visitQueue.clear();
                goto begin_flush;
            }
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                if (!curNode->children_[i]->isLeaf()) {
                    visitQueue.push_back(curNode->children_[i]);
                }
            }
        }

        // add all leaves
        visitQueue.push_back(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop_front();
            if (curNode->isLeaf()) {
                CALL_MEM_FUNC(*curNode, curNode->decompress)();
                curNode->waitForCompressAction();
                curNode->sortBuffer();
                curNode->aggregateBuffer();
                CALL_MEM_FUNC(*curNode, curNode->compress)();
                allLeaves_.push_back(curNode);
            }
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                if (!curNode->children_[i]->isLeaf()) {
                    visitQueue.push_back(curNode->children_[i]);
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
        return true;
    }

    void CompressTree::addNodeToSort(Node* node)
    {
        pthread_mutex_lock(&nodesToSortMutex_);
        if (node) {
                nodesToSort_.push_back(node);
        }
        pthread_mutex_unlock(&nodesToSortMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to to-sort list\n", node->id_);
        for (int i=0; i<nodesToSort_.size(); i++)
            fprintf(stderr, "%d, ", nodesToSort_[i]->id_);
        fprintf(stderr, "\n");
#endif
        // The node is decompressed when called.
        assert(!node->isCompressed());
    }

    bool CompressTree::wakeupSorter()
    {
        pthread_mutex_lock(&nodesToSortMutex_);
        pthread_cond_signal(&nodesReadyForSorting_);
        pthread_mutex_unlock(&nodesToSortMutex_);
        return true;
    }

    void* CompressTree::callSort()
    {
        bool rootFlag = false;
        pthread_mutex_lock(&nodesToSortMutex_);
        pthread_barrier_wait(&threadsBarrier_);
        while (nodesToSort_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "sorter sleeping\n");
#endif
            pthread_cond_wait(&nodesReadyForSorting_, 
                    &nodesToSortMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "sorter fingered\n");
#endif
            while (!nodesToSort_.empty()) {
                Node* n = nodesToSort_.front();
                nodesToSort_.pop_front();
                pthread_mutex_unlock(&nodesToSortMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "sorter: sorting node: %d\t", n->id_);
                fprintf(stderr, "remaining: ");
                for (int i=0; i<nodesToSort_.size(); i++)
                    fprintf(stderr, "%d, ", nodesToSort_[i]->id_);
                fprintf(stderr, "\n");
#endif
                n->sortBuffer();
                addNodeToEmpty(n);
                wakeupEmptier();
                pthread_mutex_lock(&nodesToSortMutex_);
            }
        }
        pthread_mutex_unlock(&nodesToSortMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Sorter quitting: %d\n", nodesToSort_.size());
#endif
        pthread_exit(NULL);
        
    }

    void* CompressTree::callCompress()
    {
        pthread_mutex_lock(&nodesReadyForCompressMutex_);
        pthread_barrier_wait(&threadsBarrier_);
        while (nodesToCompress_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "compressor sleeping\n");
#endif
            pthread_cond_wait(&nodesReadyForCompression_, &nodesReadyForCompressMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "compressor fingered\n");
#endif
            while (!nodesToCompress_.empty()) {
                Node* n = nodesToCompress_.front();
                nodesToCompress_.pop_front();
                pthread_mutex_unlock(&nodesReadyForCompressMutex_);
                pthread_mutex_lock(&(n->compActMutex_));
                if (n->compAct_ == Node::COMPRESS && !n->isCompressed()) {
                    n->snappyCompress();
                } else if (n->compAct_ == Node::DECOMPRESS) {
                    if (n->isCompressed())
                        n->snappyDecompress();
                    pthread_cond_signal(&n->compActCond_);
                }
                n->queuedForCompAct_ = false;
                n->compAct_ = Node::NONE;
                pthread_mutex_unlock(&(n->compActMutex_));
                pthread_mutex_lock(&nodesReadyForCompressMutex_);
            }
            pthread_mutex_lock(&compressionDoneMutex_);
            if (askForCompressionDoneNotice_) {
                pthread_cond_signal(&compressionDone_);
                askForCompressionDoneNotice_ = false;
            }
            pthread_mutex_unlock(&compressionDoneMutex_);
        }
        pthread_mutex_unlock(&nodesReadyForCompressMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Compressor quitting: %d\n", nodesToCompress_.size());
#endif
        pthread_exit(NULL);
    }

    void* CompressTree::callEmpty()
    {
        bool rootFlag = false;
        pthread_mutex_lock(&nodesReadyForEmptyMutex_);
        pthread_barrier_wait(&threadsBarrier_);
        while (nodesToEmpty_.empty() && !inputComplete_) {
            /* check if anybody wants a notification when list is empty */
            pthread_mutex_lock(&emptyingDoneMutex_);
            if (askForEmptyingDoneNotice_) {
                pthread_cond_signal(&emptyingDone_);
                askForEmptyingDoneNotice_ = false;
            }
            pthread_mutex_unlock(&emptyingDoneMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "emptier sleeping\n");
#endif
            pthread_cond_wait(&nodesReadyForEmptying_, 
                    &nodesReadyForEmptyMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "emptier fingered\n");
#endif
            while (!nodesToEmpty_.empty()) {
                Node* n = nodesToEmpty_.front();
                nodesToEmpty_.pop_front();
                pthread_mutex_unlock(&nodesReadyForEmptyMutex_);
                if (n->isRoot())
                    rootFlag = true;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "emptier: emptying node: %d\t", n->id_);
                fprintf(stderr, "remaining: ");
                for (int i=0; i<nodesToEmpty_.size(); i++)
                    fprintf(stderr, "%d, ", nodesToEmpty_[i]->id_);
                fprintf(stderr, "\n");
#endif
                for (int i=n->children_.size()-1; i>=0; i--) {
                    CALL_MEM_FUNC(*n->children_[i], n->children_[i]->decompress)();
                }        
                n->emptyBuffer(Node::RECURSIVE);
                if (rootFlag) {
                    // do the split and create new root
                    if (n->isLeaf())
                        handleFullLeaves();
                    rootFlag = false;
                    pthread_mutex_lock(&rootNodeAvailableMutex_);
                    pthread_cond_signal(&rootNodeAvailableForWriting_);
                    pthread_mutex_unlock(&rootNodeAvailableMutex_);
                } else {
                    // handle all the full leaves that were queued up
                    handleFullLeaves();
                }

                pthread_mutex_lock(&nodesReadyForEmptyMutex_);
            }
        }
        pthread_mutex_unlock(&nodesReadyForEmptyMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Emptier quitting: %d\n", nodesToEmpty_.size());
#endif
        pthread_exit(NULL);
    }

    void* CompressTree::callSortHelper(void *context)
    {
        return ((CompressTree*)context)->callSort();
    }

    bool CompressTree::wakeupEmptier()
    {
        pthread_mutex_lock(&nodesReadyForEmptyMutex_);
        pthread_cond_signal(&nodesReadyForEmptying_);
        pthread_mutex_unlock(&nodesReadyForEmptyMutex_);
        return true;
    }

    void CompressTree::addNodeToEmpty(Node* n)
    {
        pthread_mutex_lock(&nodesReadyForEmptyMutex_);
        if (n) {
                nodesToEmpty_.push_back(n);
        }
        pthread_mutex_unlock(&nodesReadyForEmptyMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to to-empty list\n", n->id_);
        for (int i=0; i<nodesToEmpty_.size(); i++)
            fprintf(stderr, "%d, ", nodesToEmpty_[i]->id_);
        fprintf(stderr, "\n");
#endif

        // The node is decompressed when called.
        // decompress the children
        
        assert(!n->isCompressed());
    }

    void* CompressTree::callCompressHelper(void *context)
    {
        return ((CompressTree*)context)->callCompress();
    }

    bool CompressTree::wakeupCompressor()
    {
        pthread_mutex_lock(&nodesReadyForCompressMutex_);
        pthread_cond_signal(&nodesReadyForCompression_);
        pthread_mutex_unlock(&nodesReadyForCompressMutex_);
        return true;
    }

    void* CompressTree::callEmptyHelper(void *context)
    {
        return ((CompressTree*)context)->callEmpty();
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
        pthread_attr_t attr;

        pthread_mutex_init(&nodesToSortMutex_, NULL);
        pthread_cond_init(&nodesReadyForSorting_, NULL);

        pthread_mutex_init(&nodesReadyForCompressMutex_, NULL);
        pthread_cond_init(&nodesReadyForCompression_, NULL);
        pthread_mutex_init(&compressionDoneMutex_, NULL);
        pthread_cond_init(&compressionDone_, NULL);

        pthread_mutex_init(&nodesReadyForEmptyMutex_, NULL);
        pthread_cond_init(&nodesReadyForEmptying_, NULL);

        pthread_mutex_init(&emptyingDoneMutex_, NULL);
        pthread_cond_init(&emptyingDone_, NULL);

        pthread_mutex_init(&rootNodeAvailableMutex_, NULL);
        pthread_cond_init(&rootNodeAvailableForWriting_, NULL);

        pthread_mutex_init(&evictedBufferMutex_, NULL);

        pthread_barrier_init(&threadsBarrier_, NULL, 4);

        inputComplete_ = false;

        pthread_attr_init(&attr);
        pthread_create(&compressionThread_, &attr, 
                &CompressTree::callCompressHelper, (void*)this);

        pthread_attr_init(&attr);
        pthread_create(&emptierThread_, &attr,
                &CompressTree::callEmptyHelper, (void*)this);

        pthread_attr_init(&attr);
        pthread_create(&sorterThread_, &attr,
                &CompressTree::callSortHelper, (void*)this);

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::stopThreads()
    {
        void* status;
        inputComplete_ = true;
        wakeupSorter();
        pthread_join(sorterThread_, &status);
        wakeupEmptier();
        pthread_join(emptierThread_, &status);
        wakeupCompressor();
        pthread_join(compressionThread_, &status);
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
