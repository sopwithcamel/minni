#include <assert.h>
#include <deque>
#include <pthread.h>
#include <stdint.h>
#include "Slaves.h"

namespace compresstree {
    Slave::Slave(CompressTree* tree) :
            tree_(tree),
            inputComplete_(false),
            queueEmpty_(true)
    {
        pthread_mutex_init(&queueMutex_, NULL);
        pthread_cond_init(&queueHasWork_, NULL);

        pthread_mutex_init(&completionMutex_, NULL);
        pthread_cond_init(&complete_, NULL);

    }

    bool Slave::empty() const
    {
        return queueEmpty_;
    }

    bool Slave::wakeup()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_cond_signal(&queueHasWork_);
        pthread_mutex_unlock(&queueMutex_);
        return true;
    }

    void Slave::sendCompletionNotice()
    {
        pthread_mutex_lock(&completionMutex_);
        if (askForCompletionNotice_) {
            pthread_cond_signal(&complete_);
            askForCompletionNotice_ = false;
        }
        pthread_mutex_unlock(&completionMutex_);
    }
    
    void Slave::waitUntilCompletionNoticeReceived()
    {
        pthread_mutex_lock(&queueMutex_);
        if (!empty()) {
            pthread_mutex_lock(&completionMutex_);
            askForCompletionNotice_ = true;
            pthread_mutex_unlock(&queueMutex_);

            pthread_cond_wait(&complete_, &completionMutex_);
            pthread_mutex_unlock(&completionMutex_);
        } else {
            pthread_mutex_unlock(&queueMutex_);
        }
    }

    void Slave::printElements() const
    {
        for (uint32_t i=0; i<nodes_.size(); i++) {
            fprintf(stderr, "%d, ", nodes_[i]->id());
        }
        fprintf(stderr, "\n");
    }

    Emptier::Emptier(CompressTree* tree) :
            Slave(tree)
    {
    }

    Emptier::~Emptier()
    {
    }

    void* Emptier::callHelper(void *context)
    {
        return ((Emptier*)context)->work();
    }

    void* Emptier::work()
    {
        bool rootFlag = false;
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (queue_.empty() && !inputComplete_) {
            /* check if anybody wants a notification when list is empty */
            sendCompletionNotice();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "emptier sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "emptier fingered\n");
#endif
            while (!queue_.empty()) {
                Node* n = queue_.pop();
                pthread_mutex_unlock(&queueMutex_);
                pthread_mutex_lock(&n->queuedForEmptyMutex_);
                n->queuedForEmptying_ = false;
                pthread_mutex_unlock(&n->queuedForEmptyMutex_);
                if (n->isRoot())
                    rootFlag = true;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "emptier: emptying node: %d (size: %u)\t", n->id_, n->buffer_.numElements());
                fprintf(stderr, "remaining: ");
                queue_.printElements();
#endif

                for (int i=n->children_.size()-1; i>=0; i--) {
#ifdef ENABLE_PAGING
                    // schedule pre-fetching of children of node into memory
                    tree_->pager_->pageIn(n->children_[i]);
#endif
                }        
#ifdef STRUCTURE_BUFFER
                if (n->isRoot())
                    n->aggregateSortedBuffer();
                else {
                    n->aggregateMergedBuffer();
                }
#else
                for (int i=0; i<n->children_.size(); i++) {
                    CALL_MEM_FUNC(*n->children_[i],
                            n->children_[i]->decompress)();
                }
                n->aggregateSortedBuffer();
#endif
                // check if aggregation made the node small enough
                if (!n->isFull() && !n->isRoot() && 
                        tree_->emptyType_ != ALWAYS) {
#ifdef CT_NODE_DEBUG
                    fprintf(stderr, "node: %d reduced in size to %u\n", 
                            n->id_, n->buffer_.numElements());
#endif
                    // Set node as NOT queued for emptying
                    CALL_MEM_FUNC(*n, n->compress)();
                } else {
                    n->emptyBuffer();
                    if (n->isLeaf())
                        tree_->handleFullLeaves();
                }
                if (rootFlag) {
                    // do the split and create new root
                    rootFlag = false;

                    pthread_mutex_lock(&tree_->rootNodeAvailableMutex_);
                    pthread_cond_signal(&tree_->rootNodeAvailableForWriting_);
                    pthread_mutex_unlock(&tree_->rootNodeAvailableMutex_);
                }
                pthread_mutex_lock(&queueMutex_);
            }
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Emptier quitting: %ld\n", queue_.size());
#endif
        pthread_exit(NULL);
    }

    void Emptier::addNode(Node* node)
    {
        pthread_mutex_lock(&queueMutex_);
        if (node) {
#ifdef PRIORITIZE_HIGH_NODES_FOR_EMPTY
            queue_.insert(node, node->level());
#else
            queue_.insert(node, 1);
#endif
            queueEmpty_ = false;

            std::deque<Node*> depNodes;
#ifdef PRIORITIZE_HIGH_NODES_FOR_EMPTY
            uint32_t prio = node->level();
#else
            uint32_t prio = 1;
#endif
            depNodes.push_back(node);
            while (!depNodes.empty()) {
                Node* t = depNodes.front();
                for (int i=0; i<t->children_.size(); i++) {
                    if (t->children_[i]->queuedForEmptying_) {
                        queue_.insert(t->children_[i], ++prio); 
                        depNodes.push_back(t->children_[i]);
                    }
                }
                depNodes.pop_front();
            }
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d (size: %u) added to to-empty list: ", node->id_, node->buffer_.numElements());
        queue_.printElements();
#endif
    }

    Compressor::Compressor(CompressTree* tree) :
            Slave(tree)
    {
    }

    Compressor::~Compressor()
    {
    }

    void* Compressor::callHelper(void *context)
    {
        return ((Compressor*)context)->work();
    }

    void* Compressor::work()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "compressor sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "compressor fingered\n");
#endif
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                pthread_mutex_lock(&(n->compActMutex_));
                if (n->compAct_ == Node::COMPRESS) {
                    n->snappyCompress();
#ifdef ENABLE_COUNTERS
                    tree_->monitor_->decompCtr--;
#endif
                    // signal to agent waiting for completion.
                    pthread_cond_signal(&n->compActCond_);
                } else if (n->compAct_ == Node::DECOMPRESS) {
                    pthread_mutex_unlock(&(n->compActMutex_));
#ifdef ENABLE_PAGING
                    n->waitForPageIn();
#endif
                    pthread_mutex_lock(&(n->compActMutex_));
                    n->snappyDecompress();
#ifdef ENABLE_COUNTERS
                    tree_->monitor_->decompCtr++;
#endif
                    pthread_cond_signal(&n->compActCond_);
                }
                n->queuedForCompAct_ = false;
                n->compAct_ = Node::NONE;
                pthread_mutex_unlock(&(n->compActMutex_));
                pthread_mutex_lock(&queueMutex_);
            }
            /* check if anybody wants a notification when list is empty */
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Compressor quitting: %ld\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Compressor::addNode(Node* node)
    {
        pthread_mutex_lock(&node->compActMutex_);
        if (node->compAct_ == Node::COMPRESS) {
            pthread_mutex_unlock(&node->compActMutex_);
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_back(node);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "adding node %d to compress: ", node->id_);
            printElements();
#endif
#ifdef ENABLE_PAGING
            /* Put in a request for paging out. This is necessary to do
             * right away because of the following case: if a page-in request
             * arrives when the node is on the compression queue waiting to
             * be compressed, the page-in request could simply get discarded
             * since there is no page-out request (yet). This leads to a case
             * where a decompression later assumes that the page-in has
             * completed */
            tree_->pager_->pageOut(node);
#endif
        } else {
            pthread_mutex_unlock(&node->compActMutex_);
            pthread_mutex_lock(&queueMutex_);
#ifdef PRIORITIZE_DECOMPRESSION
            nodes_.push_front(node);
#else
            nodes_.push_back(node);
#endif
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "adding node %d (size: %u) to decompress: ", node->id_, node->buffer_.numElements());
            printElements();
#endif
        }
        queueEmpty_ = false;
        pthread_mutex_unlock(&queueMutex_);
    }

    Sorter::Sorter(CompressTree* tree) :
            Slave(tree)
    {
    }

    Sorter::~Sorter()
    {
    }

    void* Sorter::callHelper(void *context)
    {
        return ((Sorter*)context)->work();
    }

    void* Sorter::work()
    {
        bool rootFlag = false;
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "sorter sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "sorter fingered\n");
#endif
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                n->waitForCompressAction(Node::DECOMPRESS);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "sorter: sorting node: %d (size: %u)\t", n->id_, n->buffer_.numElements());
                fprintf(stderr, "remaining: ");
                for (int i=0; i<nodes_.size(); i++)
                    fprintf(stderr, "%d, ", nodes_[i]->id_);
                fprintf(stderr, "\n");
#endif
#ifdef STRUCTURE_BUFFER
                if (n->isRoot())
                    n->sortBuffer();
                else {
                    n->mergeBuffer();
                }
#else
                n->sortBuffer();
#endif
                tree_->emptier_->addNode(n);
                tree_->emptier_->wakeup();
                pthread_mutex_lock(&queueMutex_);
            }
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Sorter quitting: %ld\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Sorter::addNode(Node* node)
    {
        pthread_mutex_lock(&queueMutex_);
        if (node) {
            // Set node as queued for emptying
            pthread_mutex_lock(&node->queuedForEmptyMutex_);
            node->queuedForEmptying_ = true;
            pthread_mutex_unlock(&node->queuedForEmptyMutex_);
            nodes_.push_back(node);
            queueEmpty_ = false;
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to to-sort list (size: %u)\n",
                node->id_, node->buffer_.numElements());
        for (int i=0; i<nodes_.size(); i++)
            fprintf(stderr, "%d, ", nodes_[i]->id_);
        fprintf(stderr, "\n");
#endif
    }

#ifdef ENABLE_PAGING
    Pager::Pager(CompressTree* tree) :
            Slave(tree)
    {
    }

    Pager::~Pager()
    {
    }

    void* Pager::callHelper(void *context)
    {
        return ((Pager*)context)->work();
    }

    void* Pager::work()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "pager sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "pager fingered\n");
#endif
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                pthread_mutex_lock(&(n->pageMutex_));
                if (n->pageAct_ == Node::PAGE_OUT) {
                    if (!n->isPagedOut()) {
                        if (n->isCompressed()) {
                            n->pageOut();
                            n->queuedForPaging_ = false;
                            n->pageAct_ = Node::NO_PAGE;
#ifdef CT_NODE_DEBUG
                            fprintf(stderr, "pager: paged out node: %d\n", n->id_);
#endif
                        } else {
                            pthread_mutex_lock(&queueMutex_);
                            nodes_.push_back(n);
                            pthread_mutex_unlock(&queueMutex_);
                        }
                    }
                } else if (n->pageAct_ == Node::PAGE_IN) {
                    if (n->isPagedOut()) {
                        n->pageIn();
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "pager: paged in node: %d\n", n->id_);
#endif
                     }    
                    n->queuedForPaging_ = false;
                    n->pageAct_ = Node::NO_PAGE;
                    pthread_cond_signal(&n->pageCond_);
                } else { // Node::NO_PAGE
                    n->queuedForPaging_ = false;
                }
                pthread_mutex_unlock(&(n->pageMutex_));
                pthread_mutex_lock(&queueMutex_);
            }
            /* check if anybody wants a notification when list is empty */
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Pager quitting: %ld\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Pager::pageIn(Node* node)
    {
        if (node->isPinned())
            return;
        pthread_mutex_lock(&node->pageMutex_);
        // check if node already in list
        if (node->queuedForPaging_) {
            // check if page-out request is outstanding
            if (node->pageAct_ == Node::PAGE_OUT) {
                // reset action request; node need not be added again
                node->pageAct_ = Node::NO_PAGE;
                pthread_mutex_unlock(&node->pageMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d page-out cancelled\n", node->id_);
#endif
                return;
            } else if (node->pageAct_ == Node::NO_PAGE) {
                node->pageAct_ = Node::PAGE_IN;
                pthread_mutex_unlock(&node->pageMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d reset to page-in\n", node->id_);
#endif
                return;
            } else { // we're paging-in twice
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Trying to page-in node %d twice", node->id_);
#endif
                assert(false);
            }
        } else {
            node->queuedForPaging_ = true;
            node->pageAct_ = Node::PAGE_IN;
        }
        pthread_mutex_unlock(&node->pageMutex_);
        // add the node to the page-in queue
        addNode(node);
        wakeup();
    }

    void Pager::pageOut(Node* node)
    {
        if (node->isPinned())
            return;
        pthread_mutex_lock(&node->pageMutex_);
        // check if node already in list
        if (node->queuedForPaging_) {
            // check if page-in request is outstanding
            if (node->pageAct_ == Node::PAGE_IN) {
                // reset action request; node need not be added again
                node->pageAct_ = Node::NO_PAGE;
                pthread_mutex_unlock(&node->pageMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d page-in cancelled\n", node->id_);
#endif
                return;
            } else if (node->pageAct_ == Node::NO_PAGE) {
                node->pageAct_ = Node::PAGE_OUT;
                pthread_mutex_unlock(&node->pageMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d reset to page-out\n", node->id_);
#endif
                return;
            } else { // we're paging-out twice
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Trying to page-out node %d twice", node->id_);
#endif
                assert(false);
            }
        } else {
            node->queuedForPaging_ = true;
            node->pageAct_ = Node::PAGE_OUT;
        }
        pthread_mutex_unlock(&node->pageMutex_);
        // add the node to the page queue
        addNode(node);
        wakeup();
    }

    void Pager::addNode(Node* node)
    {
        pthread_mutex_lock(&node->pageMutex_);
        if (node->pageAct_ == Node::PAGE_OUT) {
            pthread_mutex_unlock(&node->pageMutex_);
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_back(node);
        } else {
            pthread_mutex_unlock(&node->pageMutex_);
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_front(node);
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to page list: ", node->id_);
        printElements();
#endif
        queueEmpty_ = false;
        pthread_mutex_unlock(&queueMutex_);
    }
#endif //ENABLE_PAGING

#ifdef ENABLE_COUNTERS
    Monitor::Monitor(CompressTree* tree) :
            Slave(tree),
            actr(0),
            bctr(0),
            cctr(0),
            decompCtr(1)
    {
    }

    Monitor::~Monitor()
    {
    }

    void* Monitor::callHelper(void *context)
    {
        return ((Monitor*)context)->work();
    }

    void* Monitor::work()
    {
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (!inputComplete_) {
            sleep(1);
            nodeCtr.push_back(decompCtr);
            totNodeCtr.push_back(tree_->nodeCtr);
        }
        int32_t totNodes = 0;
        for (uint32_t i=0; i<nodeCtr.size(); i++) {
            totNodes += nodeCtr[i];
        }
        fprintf(stderr, "Avg. number of decomp nodes: %f\n", (float)totNodes / 
                nodeCtr.size());
        for (uint32_t i=0; i<nodeCtr.size(); i++)
            fprintf(stderr, "%d, ", nodeCtr[i]);
        fprintf(stderr, "\n");
        for (uint32_t i=0; i<totNodeCtr.size(); i++)
            fprintf(stderr, "%d, ", totNodeCtr[i]);
        fprintf(stderr, "\n");
        nodeCtr.clear();
        totNodeCtr.clear();
    }

    void Monitor::addNode(Node* n)
    {
        return;
    }
#endif
}
