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

    Emptier::Emptier(CompressTree* tree) :
            Slave(tree)
    {
    }

    Emptier::~Emptier()
    {
    }

    void* Emptier::work()
    {
        bool rootFlag = false;
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
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
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                if (n->isRoot())
                    rootFlag = true;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "emptier: emptying node: %d\t", n->id_);
                fprintf(stderr, "remaining: ");
                for (int i=0; i<nodes_.size(); i++)
                    fprintf(stderr, "%d, ", nodes_[i]->id_);
                fprintf(stderr, "\n");
#endif
                for (int i=n->children_.size()-1; i>=0; i--) {
                    CALL_MEM_FUNC(*n->children_[i], n->children_[i]->decompress)();
                }        
                n->emptyBuffer();
                if (rootFlag) {
                    // do the split and create new root
                    if (n->isLeaf())
                        tree_->handleFullLeaves();
                    rootFlag = false;
                    pthread_mutex_lock(&n->queuedForEmptyMutex_);
                    n->queuedForEmptying_ = false;
                    pthread_mutex_unlock(&n->queuedForEmptyMutex_);

                    pthread_mutex_lock(&tree_->rootNodeAvailableMutex_);
                    pthread_cond_signal(&tree_->rootNodeAvailableForWriting_);
                    pthread_mutex_unlock(&tree_->rootNodeAvailableMutex_);
                } else {
                    // handle all the full leaves that were queued up
                    tree_->handleFullLeaves();
                }

                pthread_mutex_lock(&queueMutex_);
            }
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Emptier quitting: %d\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Emptier::addNode(Node* node)
    {
        pthread_mutex_lock(&queueMutex_);
        if (node) {
                pthread_mutex_lock(&node->queuedForEmptyMutex_);
                node->queuedForEmptying_ = true;
                pthread_mutex_unlock(&node->queuedForEmptyMutex_);
                nodes_.push_back(node);
                queueEmpty_ = false;
                // schedule pre-fetching of children of node into memory
#ifdef ENABLE_PAGING
                for (int i=node->children_.size()-1; i>=0; i--) {
                    tree_->pager_->pageIn(node->children_[i]);
                }
#endif
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to to-empty list\n", node->id_);
        for (int i=0; i<nodes_.size(); i++)
            fprintf(stderr, "%d, ", nodes_[i]->id_);
        fprintf(stderr, "\n");
#endif
        // The node is decompressed when called.
        assert(!node->isCompressed());
    }

    Compressor::Compressor(CompressTree* tree) :
            Slave(tree)
    {
    }

    Compressor::~Compressor()
    {
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
                if (n->compAct_ == Node::COMPRESS && !n->isCompressed()) {
                    n->snappyCompress();
                    // signal to agent waiting for completion.
                    pthread_cond_signal(&n->compActCond_);
                } else if (n->compAct_ == Node::DECOMPRESS) {
                    if (n->isCompressed()) {
                        pthread_mutex_unlock(&(n->compActMutex_));
                        n->waitForPageIn();
                        pthread_mutex_lock(&(n->compActMutex_));
                        n->snappyDecompress();
                    }
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
        fprintf(stderr, "Compressor quitting: %d\n", nodes_.size());
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
            nodes_.push_front(node);
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
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "sorter: sorting node: %d\t", n->id_);
                fprintf(stderr, "remaining: ");
                for (int i=0; i<nodes_.size(); i++)
                    fprintf(stderr, "%d, ", nodes_[i]->id_);
                fprintf(stderr, "\n");
#endif
                n->waitForCompressAction(Node::DECOMPRESS);
                n->sortBuffer();
                tree_->emptier_->addNode(n);
                tree_->emptier_->wakeup();
                pthread_mutex_lock(&queueMutex_);
            }
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Sorter quitting: %d\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Sorter::addNode(Node* node)
    {
        pthread_mutex_lock(&queueMutex_);
        if (node) {
            nodes_.push_back(node);
            queueEmpty_ = false;
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to to-sort list\n", node->id_);
        for (int i=0; i<nodes_.size(); i++)
            fprintf(stderr, "%d, ", nodes_[i]->id_);
        fprintf(stderr, "\n");
#endif
    }

    Pager::Pager(CompressTree* tree) :
            Slave(tree)
    {
    }

    Pager::~Pager()
    {
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
        fprintf(stderr, "Pager quitting: %d\n", nodes_.size());
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
        fprintf(stderr, "Node %d added to page list\n", node->id_);
        for (int i=0; i<nodes_.size(); i++)
            fprintf(stderr, "%d, ", nodes_[i]->id_);
        fprintf(stderr, "\n");
#endif
        queueEmpty_ = false;
        pthread_mutex_unlock(&queueMutex_);
    }
}
