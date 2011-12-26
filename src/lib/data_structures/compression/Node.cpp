#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "CompressTree.h"
#include "Node.h"

namespace compresstree {

    Node::Node(NodeType typ) :
        typ_(typ),
        parent_(NULL),
        numElements_(0),
        isCompressed_(false)
    {
        // Allocate file name and create file
        // Allocate memory for data buffer
        data_ = (char*)malloc(BUFFER_SIZE);
    }

    Node::~Node()
    {
        if (data_) {
            free(data_);
            data_ = NULL;
        }            
    }

    bool Node::insert(uint64_t hash, void* buf, size_t buf_size)
    {
        // check if buffer is compressed
        if (isCompressed())
            return false;

        // copy the hash value into the buffer
        memmove(data_ + curOffset_, &hash, sizeof(hash));
        curOffset_ += sizeof(hash);

        // copy buf_size into the buffer
        memmove(data_ + curOffset_, &buf_size, sizeof(buf_size));
        curOffset_ += sizeof(buf_size);
        
        // copy the entire Block into the buffer
        memmove(data_ + curOffset_, buf, buf_size);
        curOffset_ += buf_size;

        numElements_++;
        
        // if buffer threshold is reached, call emptyBuffer
        if (isFull()) {
            sortBuffer();
            emptyBuffer();

            /* emptying the buffer could have caused recursive calls leading
             * till the leaves of the tree, but the handling of full leaves
             * is deferred until all full non-leaf buffers have been handled.
             * So we now handle the deferred tasks */
            tree->handleFullLeaves();
            curOffset_ = 0;
        }
        return true;
    }

    bool Node::emptyAllBuffers()
    {
        //TODO: Implement this!
        return true;
    }

    bool Node::isLeaf()
    {
        if (typ_ == LEAF)
            return true;
        return false;
    }

    bool Node::isRoot()
    {
        if (parent_ == NULL)
            return true;
        return false;
    }

    bool Node::emptyBuffer()
    {
        uint32_t curChild = 0;
        // offset till which elements have been written
        size_t lastOffset = 0; 
        size_t curOffset = 0;
        uint64_t* curHash;
        // set pointer to first hash value in the buffer

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf()) {
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
        data_ = NULL;
        
        // check if any children are full
        for (curChild=0; curChild < children.size(); curChild++) {
            if (children[curChild]->isFull())
                children[curChild]->emptyBuffer();
        }
        return true;
    }

    bool Node::sortBuffer()
    {
        //TODO: Implement this!
        return true;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    bool Node::splitLeaf()
    {
        size_t curOffset = 0;
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

        return parent_->addChild(median_hash, newLeaf);
    }

    bool Node::copyIntoBuffer(void* buf, size_t buf_size)
    {
        assert(curOffset_ + buf_size < BUFFER_SIZE);
        memmove(data_+curOffset_, buf, buf_size);
        return true;
    }

    bool Node::addChild(uint64_t* med, Node* newNode)   
    {
        for (uint32_t i=0; i<sepValues_.size(); i++) {
            if (*med > sepValues_[i])
                continue;
            std::vector<uint64_t>::iterator it = sepValues_.begin() + i;
            sepValues_.insert(it, *med);
            break;
        }
        // check if the number of children exceeds what is allowed
        if (children.size() > tree->b_) {
            splitNonLeaf();
        }
        return true;
    }

    /* This function will be called only when the node's buffer is empty */
    bool Node::splitNonLeaf()
    {
        assert(data_ == NULL);

        if (isRoot()) {
            // create new root
            // update tree's notion of root
        } else {
            // create new node
            Node* newNode = new Node(INTERNAL);

            // move the last floor((b+1)/2) children to new node
            int newNodeChildIndex = children.size() - (tree->b_ + 1)/2;

            // add children to new node
            for (uint32_t i=newNodeChildIndex; i<children.size(); i++)
                newNode->children.push_back(children[i]);
            // remove children from current node
            std::vector<Node*>::iterator it = children.begin() + 
                    newNodeChildIndex;
            children.erase(it, children.end());
            // add separator values to new node
            for (uint32_t i=newNodeChildIndex; i<sepValues_.size(); i++)
                newNode->sepValues_.push_back(sepValues_[i]);
            // remove separator values from current node
            std::vector<uint64_t>::iterator sep_it = sepValues_.begin() + 
                    newNodeChildIndex;
            sepValues_.erase(sep_it, sepValues_.end());
            
            // add child into parent node
            parent_->addChild(&sepValues_[sepValues_.size()-1], newNode);
            // remove separator from node
            sepValues_.pop_back();
        }
        return true;
    }

    bool Node::advance(size_t n, size_t& offset)
    {
        size_t* bufSize;
        for (uint32_t i=0; i<n; i++) {
            offset += sizeof(uint64_t);
            bufSize = (size_t*)(data_ + curOffset_);
            offset += sizeof(size_t) + *bufSize;
        }
        return true;
    }

    inline void Node::setNumElements(size_t numElements)
    {
        numElements_ = numElements;
    }

    bool Node::isFull()
    {
        if (curOffset_ > EMPTY_THRESHOLD)
            return true;
        return false;
    }

    bool Node::compress()
    {
        // TODO do compression
        isCompressed_ = true;
        return true;
    }

    bool Node::decompress()
    {
        // TODO do decompression
        isCompressed_ = false;
        return true;
    }

    bool Node::isCompressed()
    {
        return isCompressed_;
    }
}

