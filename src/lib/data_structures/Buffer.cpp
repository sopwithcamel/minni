#include <stdlib.h>
#include "Buffer.h"
#include "CompressTree.h"
#include "compsort.h"
#include "rle.h"
#include "snappy.h"

namespace compresstree {
    Buffer::List::List() :
            num_(0),
            c_hashlen_(0),
            c_sizelen_(0),
            c_datalen_(0),
            size_(0),
            state_(DECOMPRESSED)
    {
    }

    Buffer::List::~List()
    {
        deallocate();
    }

    void Buffer::List::allocate(bool isLarge)
    {
        uint32_t nel = compresstree::MAX_ELS_PER_BUFFER;
        uint32_t buf = compresstree::BUFFER_SIZE;
        if (isLarge) {
            nel *= 2;
            buf *= 2;
        }
        hashes_ = (uint32_t*)malloc(sizeof(uint32_t) * nel);
        sizes_ = (uint32_t*)malloc(sizeof(uint32_t) * nel);
        data_ = (char*)malloc(buf);
    }

    void Buffer::List::deallocate()
    {    
        if (hashes_) { free(hashes_); hashes_ = NULL;}
        if (sizes_) { free(sizes_); sizes_ = NULL;}
        if (data_) { free(data_); data_ = NULL;}
    }

    void Buffer::List::setEmpty()
    {
        num_ = 0;
        size_ = 0;
        state_ = DECOMPRESSED;
    }

    Buffer::Buffer() :
        compressible_(true),
        queuedForCompAct_(false),
        compAct_(NONE)
    {
        pthread_mutex_init(&compActMutex_, NULL);
        pthread_cond_init(&compActCond_, NULL);
    }

    Buffer::~Buffer()
    {
        deallocate();

        pthread_mutex_destroy(&compActMutex_);
        pthread_cond_destroy(&compActCond_);
    }

    Buffer::List* Buffer::addList(bool isLarge/*=false*/)
    {
        List *l = new List();
        l->allocate(isLarge);
        lists_.push_back(l);
        return l;
    }

    void Buffer::delList(uint32_t ind)
    {
        if (ind < lists_.size()) {
            delete lists_[ind];
            lists_.erase(lists_.begin() + ind);
        }
    }

    void Buffer::addList(Buffer::List* l)
    {
        lists_.push_back(l);
    }

    void Buffer::clear()
    {
        lists_.clear();
    }

    void Buffer::deallocate()
    {
        for (uint32_t i=0; i<lists_.size(); i++)
            lists_[i]->deallocate();
    }

    bool Buffer::empty() const
    {
        return (numElements() == 0);
    }

    bool Buffer::compress()
    {
        if (!empty()) {
            // allocate memory for one list
            Buffer compressed;

            for (uint32_t i=0; i<lists_.size(); i++) {
                Buffer::List* l = lists_[i];
                if (l->state_ == Buffer::List::COMPRESSED)
                    continue;
                compressed.addList();
                // latest added list
                Buffer::List* cl = 
                        compressed.lists_[compressed.lists_.size()-1];
                snappy::RawCompress((const char*)l->hashes_, 
                        l->num_ * sizeof(uint32_t), 
                        (char*)cl->hashes_,
                        &l->c_hashlen_);
                snappy::RawCompress((const char*)l->sizes_, 
                        l->num_ * sizeof(uint32_t), 
                        (char*)cl->sizes_,
                        &l->c_sizelen_);
/*
                compsort::compress(l->hashes_, l->num_,
                        cl->hashes_, (uint32_t&)l->c_hashlen_);
                rle::encode(l->sizes_, l->num_, cl->sizes_,
                        (uint32_t&)l->c_sizelen_);
*/
                snappy::RawCompress(l->data_, l->size_, 
                        cl->data_, 
                        &l->c_datalen_);
                l->deallocate();
                l->hashes_ = cl->hashes_;
                l->sizes_ = cl->sizes_;
                l->data_ = cl->data_;
                l->state_ = Buffer::List::COMPRESSED;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "compressed list %d in node %d\n", i, id_);
#endif
            }
            // clear compressed list so lists won't be deallocated on return
            compressed.clear();
        }
        return true;
    }

    bool Buffer::decompress()
    {
        if (!empty()) {
            // allocate memory for decompressed buffers
            Buffer decompressed;

            for (uint32_t i=0; i<lists_.size(); i++) {
                Buffer::List* cl = lists_[i];
                if (cl->state_ == Buffer::List::DECOMPRESSED)
                    continue;
                decompressed.addList();
                // latest added list
                Buffer::List* l =
                        decompressed.lists_[decompressed.lists_.size()-1];
                snappy::RawUncompress((const char*)cl->hashes_, 
                        cl->c_hashlen_, (char*)l->hashes_);
                snappy::RawUncompress((const char*)cl->sizes_, 
                        cl->c_sizelen_, (char*)l->sizes_);
/*
                uint32_t siz;
                compsort::decompress(cl->hashes_, (uint32_t)cl->c_hashlen_,
                        l->hashes_, siz);
                rle::decode(cl->sizes_, (uint32_t)cl->c_sizelen_,
                        l->sizes_, siz);
*/
                snappy::RawUncompress(cl->data_, cl->c_datalen_,
                        l->data_);
                cl->deallocate();
                cl->hashes_ = l->hashes_;
                cl->sizes_ = l->sizes_;
                cl->data_ = l->data_;
                cl->state_ = Buffer::List::DECOMPRESSED;
            }
            // clear decompressed so lists won't be deallocated on return
            decompressed.clear();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "decompressed node %d; n: %u\n", id_, buffer_.numElements());
#endif
        }
        return true;
    }

    void Buffer::setCompressible(bool flag)
    {
        compressible_ = flag;
    }

    bool Buffer::scheduleCompress()
    {
        pthread_mutex_lock(&compActMutex_);
        // check if node already in compression action list
        if (queuedForCompAct_) {
            // check if compression request has been cancelled
            if (compAct_ == DECOMPRESS) {
                /* reset action request; node need not be added
                 * again */
                compAct_ = NONE;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d decompression cancelled\n", id_);
#endif
                pthread_mutex_unlock(&compActMutex_);
                return true;
            } else if (compAct_ == NONE) {
                compAct_ = COMPRESS;
                pthread_mutex_unlock(&compActMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d reset to compress\n", id_);
#endif
                return true;
            } else { // we're compressing twice
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Trying to compress node %d twice", id_);
#endif
//                assert(false);
            }
        } else {
            if (empty()) {
                queuedForCompAct_ = false;
                pthread_mutex_unlock(&compActMutex_);
                return true;
            } else {
                queuedForCompAct_ = true;
                compAct_ = COMPRESS;
            }
        }
        pthread_mutex_unlock(&compActMutex_);
        return true;
    }
    
    bool Buffer::scheduleDecompress()
    {
        pthread_mutex_lock(&compActMutex_);
        // check if node already in list
        if (queuedForCompAct_) {
            // check if compression request is outstanding
            if (compAct_ == COMPRESS || compAct_ == NONE) {
                compAct_ = DECOMPRESS;
                pthread_mutex_unlock(&compActMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d reset to decompress\n", id_);
#endif
                return true;
            } else { // we're decompressing twice
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Trying to decompress node %d twice", id_);
#endif
                assert(false);
            }
        } else {
            // check if the buffer is empty;
            if (empty()) {
                queuedForCompAct_ = false;
                pthread_mutex_unlock(&compActMutex_);
                return true;
            } else {
                queuedForCompAct_ = true;
                compAct_ = DECOMPRESS;
            }
        }
        pthread_mutex_unlock(&compActMutex_);
        return true;
    }
    
    void Buffer::waitForCompressAction(const CompressionAction& act)
    {
        // make sure the buffer has been decompressed
        pthread_mutex_lock(&compActMutex_);
        while (queuedForCompAct_ && compAct_ == act)
            pthread_cond_wait(&compActCond_, &compActMutex_);
        pthread_mutex_unlock(&compActMutex_);
    }

    void Buffer::performCompressAction()
    {
        pthread_mutex_lock(&compActMutex_);
        if (compAct_ == COMPRESS) {
            compress();
            // signal to agent waiting for completion.
            pthread_cond_signal(&compActCond_);
        } else if (compAct_ == DECOMPRESS) {
            pthread_mutex_unlock(&compActMutex_);
#ifdef ENABLE_PAGING
            waitForPageIn();
#endif
            pthread_mutex_lock(&compActMutex_);
            decompress();
            pthread_cond_signal(&compActCond_);
        }
        queuedForCompAct_ = false;
        compAct_ = NONE;
        pthread_mutex_unlock(&compActMutex_);
    }

    Buffer::CompressionAction Buffer::getCompressAction()
    {
        pthread_mutex_lock(&compActMutex_);
        CompressionAction act = compAct_;
        pthread_mutex_unlock(&compActMutex_);
        return act;
    }    

    uint32_t Buffer::numElements() const
    {
        uint32_t num = 0;
        for (uint32_t i=0; i<lists_.size(); i++)
            num += lists_[i]->num_;
        return num;
    }

}
