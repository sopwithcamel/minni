#include "Block.h"
#include "CompressTree.h"
#include "Node.h"

namespace compresstree {

    Node::Node() :
        numBlocks_(0),
        isInMemory(false)
    {
        // Allocate file name and create file
    }

    Node::~Node()
    {
    }

    bool Node::insert(uint64_t hash, void* buf, size_t buf_size)
    {
        // check if buffer is compressed
        assert(!isCompressed());

        // copy the hash value into the buffer
        memmove(data + curOffset_, &hash, sizeof(hash));
        curOffset_ += sizeof(hash);

        // copy buf_size into the buffer
        memmove(data + curOffset_, &buf_size, sizeof(buf_size));
        curOffset_ += sizeof(buf_size);
        
        // copy the entire Block into the buffer
        memmove(data + curOffset_, buf, buf_size);
        curOffset_ += buf_size;
        
        // if buffer threshold is reached, call emptyBuffer
        if (isFull()) {
            sortBuffer();
            emptyBuffer();
            handleFullLeaves();
            curOffset_ = 0;
        }
    }

    bool Node::emptyAllBuffers()
    {
    }

    bool Node::emptyBuffer()
    {
        int curChild = 0;
        void* childBuf = NULL;
        // offset till which elements have been written
        size_t lastOffset = 0; 
        size_t curOffset = 0;
        uint64_t* curHash;
        size_t* bufSize;
        size_t curElement = 0;
        // set pointer to first hash value in the buffer

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf) {
            tree->addLeafToEmpty(this);
            return true;
        }

        while (curOffset < curOffset_) {
            curHash = (uint64_t*)(data_ + curOffset);
            while (*curHash <= sepValues_[curChild]) {
                curOffset += sizeof(uint64_t);
                bufSize = (size_t*)(data_ + curOffset);
                curOffset += sizeof(size_t) + *bufSize;
                curHash = (uint64_t*)(data_ + curOffset);
            }
            if (curOffset > lastOffset) { // at least one element for this
                assert(children[curChild]->decompress());
                assert(children[curChild]->copyIntoBuffer(data_ + lastOffset,
                            curOffset - lastOffset));
                assert(children[curChild]->compress());
                lastOffset = curOffset;
            }
            curChild++;
        }
        // release memory
        free(data_);
        
        // check if any children are full
        for (curChild=0; curChild < children.size(); curChild++) {
            if (children[curChild]->isFull())
                children[curChild]->emptyBuffer();
        }
        return true;
    }

    bool Node::sortBuffer()
    {
    }

    bool Node::isFull()
    {
        if (curOffset_ > EMPTY_THRESHOLD) {
            return true;
        return false;
    }


    bool Node::compress()
    {
        isCompressed = true;
    }

    bool Node::decompress()
    {
        isCompressed = false;
    }

    bool Node::isCompressed()
    {
        return isCompressed;
    }
}

