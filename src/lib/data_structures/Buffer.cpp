#include <stdlib.h>
#include "Buffer.h"
#include "CompressTree.h"

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

    void Buffer::List::allocate()
    {
        hashes_ = (uint32_t*)malloc(sizeof(uint32_t) * 
                compresstree::MAX_ELS_PER_BUFFER);
        sizes_ = (uint32_t*)malloc(sizeof(uint32_t) *
                compresstree::MAX_ELS_PER_BUFFER);
        data_ = (char*)malloc(BUFFER_SIZE);
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

    Buffer::Buffer()
    {
    }

    Buffer::~Buffer()
    {
        deallocate();
    }

    Buffer::List* Buffer::addList()
    {
        List *l = new List();
        l->allocate();
        lists_.push_back(l);
        return l;
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

    uint32_t Buffer::numElements() const
    {
        uint32_t num = 0;
        for (uint32_t i=0; i<lists_.size(); i++)
            num += lists_[i]->num_;
        return num;
    }

}