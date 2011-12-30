#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "CompressTree.h"
#include "Node.h"

namespace compresstree {
    static uint32_t nodeCtr = 0;

    Node::Node(NodeType typ, CompressTree* tree) :
        tree_(tree),
        typ_(typ),
        id_(nodeCtr++),
        parent_(NULL),
        numElements_(0),
        curOffset_(0),
        isCompressed_(false)
    {
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
//        fprintf(stderr, "value at %ld", curOffset_);
        curOffset_ += sizeof(hash);

        // copy buf_size into the buffer
        memmove(data_ + curOffset_, &buf_size, sizeof(buf_size));
//        fprintf(stderr, ", size at %ld", curOffset_);
        curOffset_ += sizeof(buf_size);
        
        // copy the entire Block into the buffer
        memmove(data_ + curOffset_, buf, buf_size);
//        fprintf(stderr, ", data at %ld\n", curOffset_);
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
            tree_->handleFullLeaves();
        }
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
        size_t offset = 0;
        volatile uint64_t* curHash;
        size_t numCopied = 0;

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf()) {
            tree_->addLeafToEmpty(this);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d added to full-leaf-list\n", id_);
#endif
            return true;
        }

        // find the first separator greater than the first element
        curHash = (uint64_t*)(data_ + offset);
        while (*curHash >= sepValues_[curChild])
            curChild++;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: first node chosen: %d (sep: %lu, child: %d); first element: %ld\n", id_, children_[curChild]->id_, sepValues_[curChild], curChild, *curHash);
#endif

        while (offset < curOffset_) {
            advance(1, offset);
            numCopied++;
            curHash = (uint64_t*)(data_ + offset);

            if (offset >= curOffset_ || *curHash >= sepValues_[curChild]) {
                assert(curChild < children_.size());
                if (offset > lastOffset) { // at least one element for this
                    assert(children_[curChild]->decompress());
                    assert(children_[curChild]->copyIntoBuffer(data_ + lastOffset,
                                offset - lastOffset));
                    children_[curChild]->addElements(numCopied);
                    assert(children_[curChild]->compress());
#ifdef CT_NODE_DEBUG
                    fprintf(stderr, "Copied %lu elements into node %d; offset: %ld\n", numCopied, children_[curChild]->id_, children_[curChild]->curOffset_);
#endif
                    lastOffset = offset;
                    numCopied = 0;
                }
                curChild++;
            }
        }
        // reset
        curOffset_ = 0;
        numElements_ = 0;
        
        // check if any children are full
        for (curChild=0; curChild < children_.size(); curChild++) {
            if (children_[curChild]->isFull())
                children_[curChild]->emptyBuffer();
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
        size_t offset = 0;
        if (!advance(numElements_/2, offset))
            return false;

        // select median value
        uint64_t* median_hash = (uint64_t*)(data_ + offset);

        // create new leaf
        Node* newLeaf = new Node(LEAF, tree_);
        newLeaf->copyIntoBuffer(data_ + offset, curOffset_ - offset);
        newLeaf->addElements(numElements_ - numElements_/2);
        newLeaf->compress();

        // set this leaf properties
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new offsets: %ld and %ld\n", id_, newLeaf->id_, offset, curOffset_ - offset);
#endif
        curOffset_ = offset;
        reduceElements(numElements_ - numElements_/2);

        // if leaf is also the root, create new root
        if (isRoot()) {
            return tree_->createNewRoot(*median_hash, newLeaf);
        } else {
            return parent_->addChild(*median_hash, newLeaf, RIGHT);
        }
    }

    bool Node::copyIntoBuffer(void* buf, size_t buf_size)
    {
        assert(curOffset_ + buf_size < BUFFER_SIZE);
        memmove(data_+curOffset_, buf, buf_size);
        curOffset_ += buf_size;
        return true;
    }

    bool Node::addChild(uint64_t med, Node* newNode, ChildType ctyp)   
    {
        uint32_t i;
        // insert separator value

        // find position of insertion
        std::vector<uint64_t>::iterator it = sepValues_.begin();
        for (i=0; i<sepValues_.size(); i++) {
            if (med > sepValues_[i])
                continue;
            break;
        }
        it = sepValues_.begin() + i;
        sepValues_.insert(it, med);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Separator %lu added at pos %u, [", id_, med, i);
        for (uint32_t j=0; j<sepValues_.size(); j++)
            fprintf(stderr, "%lu, ", sepValues_[j]);
        fprintf(stderr, "]\n");
#endif

        // insert child
        std::vector<Node*>::iterator ch_it = children_.begin() + i;
        if (ctyp == RIGHT)
            ch_it++;
        children_.insert(ch_it, newNode);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Node %d added at pos %u, [", id_, newNode->id_, i);
        for (uint32_t j=0; j<children_.size(); j++)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "]\n");
#endif

        // set parent
        newNode->parent_ = this;

        // check if the number of children exceeds what is allowed
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
        return true;
    }

    /* This function will be called only when the node's buffer is empty */
    bool Node::splitNonLeaf()
    {
        // create new node
        Node* newNode = new Node(NON_LEAF, tree_);

        // move the last floor((b+1)/2) children to new node
        int newNodeChildIndex = children_.size()-(tree_->b_+1)/2;

        // add children to new node
        for (uint32_t i=newNodeChildIndex; i<children_.size(); i++) {
            newNode->children_.push_back(children_[i]);
            children_[i]->parent_ = newNode;
        }

        // remove children from current node
        std::vector<Node*>::iterator it = children_.begin() + 
                newNodeChildIndex;
        children_.erase(it, children_.end());
        // add separator values to new node
        for (uint32_t i=newNodeChildIndex; i<sepValues_.size(); i++)
            newNode->sepValues_.push_back(sepValues_[i]);
        // remove separator values from current node
        std::vector<uint64_t>::iterator sep_it = sepValues_.begin() + 
                newNodeChildIndex;
        sepValues_.erase(sep_it, sepValues_.end());

        // median separator from node
        uint64_t med = sepValues_.back();
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "After split [");
        for (uint32_t j=0; j<sepValues_.size(); j++)
            fprintf(stderr, "%lu, ", sepValues_[j]);
        fprintf(stderr, "] and [");
        for (uint32_t j=0; j<newNode->sepValues_.size(); j++)
            fprintf(stderr, "%lu, ", newNode->sepValues_[j]);
        fprintf(stderr, "]\n");

        fprintf(stderr, "Children [");
        for (uint32_t j=0; j<children_.size(); j++)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "] and [");
        for (uint32_t j=0; j<newNode->children_.size(); j++)
            fprintf(stderr, "%d, ", newNode->children_[j]->id_);
        fprintf(stderr, "]\n");
#endif

        if (isRoot())
            return tree_->createNewRoot(med, newNode);
        else
            return parent_->addChild(med, newNode, RIGHT);
    }

    bool Node::advance(size_t n, size_t& offset)
    {
        size_t* bufSize;
        for (uint32_t i=0; i<n; i++) {
            offset += sizeof(uint64_t);
            bufSize = (size_t*)(data_ + offset);
            offset += sizeof(size_t) + *bufSize;
        }
        return true;
    }

    inline void Node::addElements(size_t numElements)
    {
        numElements_ += numElements;
    }

    inline void Node::reduceElements(size_t numElements)
    {
        numElements_ -= numElements;
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

