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
                nodes_.push_back(node);
                queueEmpty_ = false;
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
                } else if (n->compAct_ == Node::DECOMPRESS) {
                    if (n->isCompressed())
                        n->snappyDecompress();
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
        } else {
            pthread_mutex_unlock(&node->compActMutex_);
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_front(node);
        }
        queueEmpty_ = false;
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "call for compressing node %d\t", node->id_);
        for (int i=0; i<nodes_.size(); i++)
            fprintf(stderr, "%d, ", nodes_[i]->id_);
        fprintf(stderr, "\n");
#endif
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
                n->waitForCompressAction();
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
}
