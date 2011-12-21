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

        numElements_++;
        
        // if buffer threshold is reached, call emptyBuffer
        if (isFull()) {
            sortBuffer();
            emptyBuffer();
            tree->handleFullLeaves();
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
                advance(1, curOffset);
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

    bool Node::splitNode()
    {
        if (isLeaf)
            return splitLeaf();
        else
            return splitInternal();
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    bool Node::splitLeaf()
    {
        size_t curOffset;
        if (!advance(numElements_/2, curOffset))
            return false;

        // select median value
        uint64_t* median_hash = (uint64_t*)curOffset;

        // create new leaf
        Node* newLeaf = new Node(LEAF);
        newLeaf->copyIntoBuffer(data_ + curOffset, curOffset_ - curOffset);
        newLeaf->setNumElements(numElements_ - numElements_/2);
        newLeaf->compress();

        // set this leaf properties
        curOffset_ = curOffset;
        setNumElements(numElements_ / 2);

        return parent->addChild(median_hash, newLeaf);
    }

    bool Node::addChild(uint64_t* med, Node* newNode)   
    {
        for (int i=0; i<sepValues_.size(); i++) {
            if (*med > sepValues_[i])
                continue;
            std::vector<Node*>::iterator it = sepValues_.begin() + i;
            sepValues_.insert(it, *med);
            break;
        }
        if (children.size() > tree->b) {
        }
        return true;
    }

    bool Node::splitNonLeaf()
    {
        if (isRoot()) {
            // create new root
            // update tree's notion of root
        }
    }

    bool Node::shareChildren()
    {
    }

    bool Node::advance(size_t n, size_t& offset)
    {
        size_t bufSize;
        for (int i=0; i<n; i++) {
            offset += sizeof(uint64_t);
            bufSize = (size_t*)(data_ + curOffset);
            offset += sizeof(size_t) + *bufSize;
        }
    }

    inline void Node::setNumElements(size_t numElements)
    {
        numElements_ = numElements;
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

