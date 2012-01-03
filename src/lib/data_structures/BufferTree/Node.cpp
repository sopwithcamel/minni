#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "BufferTree.h"
#include "Node.h"

namespace buffertree {
    static uint32_t nodeCtr = 0;

    Node::Node(NodeType typ, BufferTree* tree) :
        tree_(tree),
        typ_(typ),
        id_(nodeCtr++),
        parent_(NULL),
        numElements_(0),
        curOffset_(0),
        isFlushed_(false),
        flushable_(true)
    {
        // Allocate memory for data buffer
        data_ = (char*)malloc(BUFFER_SIZE);
        char* fileName = (char*)malloc(FILENAME_LENGTH);
        char* nodeNum = (char*)malloc(10);
        strcpy(fileName, "/tmp/");
        sprintf(nodeNum, "%d", id_);
        strcat(fileName, nodeNum);
        strcat(fileName, ".buf");
        fd_ = open(fileName, O_CREAT|O_RDWR|O_TRUNC, 0755);
        free(fileName);
        free(nodeNum);
    }

    Node::~Node()
    {
        if (data_) {
            free(data_);
            data_ = NULL;
        }
        close(fd_);
    }

    bool Node::insert(uint64_t hash, void* buf, size_t buf_size)
    {
        // check if buffer is in flushed state
        if (isFlushed())
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
            /* this may be called even when buffer is not full (when flushing
             * all buffers at the end). */
            if (isFull()) {
                tree_->addLeafToEmpty(this);
#ifdef BT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\n", id_);
#endif
            }
            return true;
        }

        if (curOffset_ == 0)
            goto emptyChildren;

        load();
        sortBuffer();
        // find the first separator greater than the first element
        curHash = (uint64_t*)(data_ + offset);
        while (*curHash >= children_[curChild]->separator_) {
            curChild++;
            if (curChild >= children_.size()) {
                fprintf(stderr, "Node: %d: Can't place %ld among children\n", id_, *curHash);
                checkIntegrity();
                assert(false);
            }
        }
#ifdef BT_NODE_DEBUG
        fprintf(stderr, "Node: %d: first node chosen: %d (sep: %lu,\
            child: %d); first element: %ld\n", id_, children_[curChild]->id_,
            children_[curChild]->separator_, curChild, *curHash);
#endif
        while (offset < curOffset_) {
            curHash = (uint64_t*)(data_ + offset);
            size_t bs = *(size_t*)(data_ + offset + 8);
            assert(bs > 0);

            if (offset >= curOffset_ || *curHash >= children_[curChild]->separator_) {
                // this separator is the largest separator that is not greater
                // than *curHash. This invariant needs to be maintained.
                if (numCopied > 0) { // at least one element for this
                    assert(children_[curChild]->load());
                    assert(children_[curChild]->copyIntoBuffer(data_ + 
                                lastOffset, offset - lastOffset));
                    children_[curChild]->addElements(numCopied);
                    assert(children_[curChild]->flush());
#ifdef BT_NODE_DEBUG
                    fprintf(stderr, "Copied %lu elements into node %d; child\
                            offset: %ld, sep: %lu, off:(%ld/%ld)\n", numCopied, 
                            children_[curChild]->id_,
                            children_[curChild]->curOffset_,
                            children_[curChild]->separator_,
                            offset, curOffset_);
#endif
                    lastOffset = offset;
                    numCopied = 0;
                }
                // skip past all separators not greater than *curHash
                while (*curHash >= children_[curChild]->separator_) {
                    curChild++;
                    if (curChild >= children_.size()) {
                        fprintf(stderr, "Can't place %ld among children\n", *curHash);
                        assert(false);
                    }
                }
            }
            // proceed to next element
            advance(1, offset);
            numCopied++;
        }

        // copy remaining elements into child
        if (offset >= lastOffset) {
            children_[curChild]->load();
            assert(children_[curChild]->copyIntoBuffer(data_ + 
                        lastOffset, offset - lastOffset));
            children_[curChild]->addElements(numCopied);
            children_[curChild]->flush();
        }

        // reset
        curOffset_ = 0;
        numElements_ = 0;

        if (!isRoot()) {
            free(data_);
            data_ = NULL;
        }

emptyChildren:
        // check if any children are full
        for (curChild=0; curChild < children_.size(); curChild++) {
            if (children_[curChild]->isFull()) {
                children_[curChild]->emptyBuffer();
            }
        }
        tree_->handleFullLeaves();
        return true;
    }

    void Node::swapPointers(uint64_t*& el1, uint64_t*& el2)
    {
        uint64_t* temp = el1;   
        el1 = el2;
        el2 = temp;
    }

    void Node::verifySort()
    {
#ifdef ENABLE_SORT_VERIFICATION
        size_t offset = 0;
        uint64_t* curHash, *nextHash;
        curHash = (uint64_t*)(data_ + offset);
        while (offset < curOffset_) {
            advance(1, offset);
            if (offset >= curOffset_)
                break;
            nextHash = (uint64_t*)(data_ + offset);
            if (*curHash > *nextHash) {
                fprintf(stderr, "Node %d not sorted\n", id_);
                fprintf(stderr, "%ld before %ld\n", *curHash, *nextHash);
                assert(false);
            }
            curHash = nextHash;
        }
#endif
    }

    void Node::setSeparator(uint64_t sep)
    {
        separator_ = sep;
    }

    size_t Node::partition(uint64_t** arr, size_t left, size_t right,
            size_t pivIndex)
    {
        size_t storeIndex = left;
        uint64_t pivotValue = *arr[pivIndex];
        // swap pivot element and right-most element
        swapPointers(arr[pivIndex], arr[right]);
        for (size_t i=left; i<right; i++) {
            if (*arr[i] < pivotValue) {
                swapPointers(arr[i], arr[storeIndex]);
                storeIndex++;
            }
        }
        // move pivot to final place
        swapPointers(arr[storeIndex], arr[right]);
        return storeIndex;
    }

    void Node::quicksort(uint64_t** arr, size_t left, size_t right)
    {
        size_t pivIndex, newPivIndex;
        if (left < right) {
            pivIndex = (left + right) / 2;
            newPivIndex = partition(arr, left, right, pivIndex);
            if (left < newPivIndex)
                quicksort(arr, left, newPivIndex - 1);
            if (newPivIndex < right)
                quicksort(arr, newPivIndex + 1, right);
        }
    }

    bool Node::sortBuffer()
    {
        size_t offset = 0;
        size_t el_size;
        size_t auxOffset = 0;
        size_t auxEls = 0;
        size_t buf_size;

        if (numElements_ == 0)
            return true;
#ifdef ENABLE_ASSERT_CHECKS
        if (isFlushed()) {
            fprintf(stderr, "Node %d is flushed still!\n", id_);
            assert(false);
        }
#endif

        for (uint64_t i=0; i<numElements_; i++) {
            tree_->els_[i] = (uint64_t*)(data_ + offset);
            if (!advance(1, offset))
                return false;
        }

        // quicksort elements
        quicksort(tree_->els_, 0, numElements_ - 1);

        // aggregate elements in buffer
        uint64_t lastIndex = 0;
        for (uint64_t i=1; i<numElements_; i++) {
            if (*(tree_->els_[i]) == *(tree_->els_[lastIndex])) {
                // aggregate elements
            } else {
                // copy into auxBuffer_
                buf_size = *(size_t*)(tree_->els_[lastIndex] + 1);
                el_size = sizeof(uint64_t) + sizeof(size_t) + buf_size;
                memmove(tree_->auxBuffer_ + auxOffset, 
                        (void*)(tree_->els_[lastIndex]), el_size);
                auxOffset += el_size;
                auxEls++;

                lastIndex = i;
            }
        }
        buf_size = *(size_t*)(tree_->els_[lastIndex] + 1);
        el_size = sizeof(uint64_t) + sizeof(size_t) + buf_size;
        memmove(tree_->auxBuffer_ + auxOffset, 
                (void*)(tree_->els_[lastIndex]), el_size);
        auxOffset += el_size;
        auxEls++;

        // swap buffer pointers
        char* tp = data_;
        data_ = tree_->auxBuffer_;
        tree_->auxBuffer_ = tp;
        curOffset_ = auxOffset;
        numElements_ = auxEls;
        return true;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::splitLeaf()
    {
        size_t offset = 0;
        sortBuffer();
        if (!advance(numElements_/2, offset))
            return false;

        // select median value
        uint64_t* median_hash = (uint64_t*)(data_ + offset);

        // create new leaf
        Node* newLeaf = new Node(LEAF, tree_);
        newLeaf->copyIntoBuffer(data_ + offset, curOffset_ - offset);
        newLeaf->addElements(numElements_ - numElements_/2);
        newLeaf->separator_ = separator_;
        newLeaf->checkIntegrity();

        // set this leaf properties
#ifdef BT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new offsets: %ld and\
                %ld\n", id_, newLeaf->id_, offset, curOffset_ - offset);
#endif
        curOffset_ = offset;
        reduceElements(numElements_ - numElements_/2);
        separator_ = *median_hash;
        checkIntegrity();

        // if leaf is also the root, create new root
        if (isRoot()) {
            setFlushable(true);
            tree_->createNewRoot(newLeaf);
        } else {
            parent_->addChild(newLeaf);
        }
        return newLeaf;
    }

    bool Node::copyIntoBuffer(void* buf, size_t buf_size)
    {
#ifdef ENABLE_ASSERT_CHECKS
        if (curOffset_ + buf_size >= BUFFER_SIZE) {
            fprintf(stderr, "Node: %d, cOffset: %ld, buf: %ld\n", id_, 
                    curOffset_, buf_size); 
            assert(false);
        }
        if (data_ == NULL) {
            fprintf(stderr, "data buffer is null\n");
            assert(false);
        }
#endif
        memmove(data_+curOffset_, buf, buf_size);
        curOffset_ += buf_size;
        return true;
    }

    bool Node::addChild(Node* newNode)   
    {
        uint32_t i;
        // insert separator value

        // find position of insertion
        std::vector<Node*>::iterator it = children_.begin();
        for (i=0; i<children_.size(); i++) {
            if (newNode->separator_ > children_[i]->separator_)
                continue;
            break;
        }
        it += i;
        children_.insert(it, newNode);
#ifdef BT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Node %d added at pos %u, [", id_, 
                newNode->id_, i);
        for (uint32_t j=0; j<children_.size(); j++)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "], num children: %ld\n", children_.size());
#endif

        // set parent
        newNode->parent_ = this;

        // check if the number of children exceeds what is allowed
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }

        return true;
    }

    bool Node::splitNonLeaf()
    {
        // Ensure that the node's buffer is empty
        if (numElements_ > 0) {
            emptyBuffer();
            tree_->handleFullLeaves();
        }

        // create new node
        Node* newNode = new Node(NON_LEAF, tree_);
        // move the last floor((b+1)/2) children to new node
        int newNodeChildIndex = children_.size()-(tree_->b_+1)/2;
        // add children to new node
        for (uint32_t i=newNodeChildIndex; i<children_.size(); i++) {
            newNode->children_.push_back(children_[i]);
            children_[i]->parent_ = newNode;
        }
        // set separator
        newNode->separator_ = separator_;

        // remove children from current node
        std::vector<Node*>::iterator it = children_.begin() + 
                newNodeChildIndex;
        children_.erase(it, children_.end());

        // median separator from node
        separator_ = children_[children_.size()-1]->separator_;
#ifdef BT_NODE_DEBUG
        fprintf(stderr, "After split, %d: [", id_);
        for (uint32_t j=0; j<children_.size(); j++)
            fprintf(stderr, "%lu, ", children_[j]->separator_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j=0; j<newNode->children_.size(); j++)
            fprintf(stderr, "%lu, ", newNode->children_[j]->separator_);
        fprintf(stderr, "]\n");

        fprintf(stderr, "Children, %d: [", id_);
        for (uint32_t j=0; j<children_.size(); j++)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j=0; j<newNode->children_.size(); j++)
            fprintf(stderr, "%d, ", newNode->children_[j]->id_);
        fprintf(stderr, "]\n");
#endif

        if (isRoot()) {
            setFlushable(true);
            return tree_->createNewRoot(newNode);
        } else
            return parent_->addChild(newNode);
    }

    bool Node::advance(size_t n, size_t& offset)
    {
        size_t* bufSize;
        for (uint32_t i=0; i<n; i++) {
            offset += sizeof(uint64_t);
            bufSize = (size_t*)(data_ + offset);
            offset += sizeof(size_t) + *bufSize;
            if (*bufSize == 0) {
                fprintf(stderr, "%lu, bufsize: %lu, %ld\n", offset, *bufSize, *(size_t*)(data_ + offset + 16));
                assert(false);
            }
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

    bool Node::flush()
    {
#ifdef ENABLE_FLUSHING
        size_t ret;
        if (!flushable_)
            return true;
        ret = pwrite(fd_, data_, curOffset_, 0);
#ifdef ENABLE_ASSERT_CHECKS
        assert(ret == curOffset_);
#endif
#ifdef BT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Flushed %ld bytes\n", id_, ret); 
#endif
        sync();
        free(data_);
        data_ = NULL;
        isFlushed_ = true;
#endif
        return true;
    }

    bool Node::load()
    {
        if (data_ == NULL)
            data_ = (char*)malloc(BUFFER_SIZE);
#ifdef ENABLE_FLUSHING
        if (!flushable_)
            return true;
        size_t ret = pread(fd_, data_, curOffset_, 0);
#ifdef ENABLE_ASSERT_CHECKS
        assert(ret == curOffset_);
#endif
        isFlushed_ = false;
#endif
        return true;
    }

    bool Node::isFlushed()
    {
        return isFlushed_;
    }

    void Node::setFlushable(bool flag)
    {
        flushable_ = flag;
    }

    bool Node::checkIntegrity()
    {
#ifdef ENABLE_INTEGRITY_CHECK
        size_t offset;
        uint64_t* curHash;
        offset = 0;
        while (offset < curOffset_) {
            curHash = (uint64_t*)(data_ + offset);
            if (*curHash >= separator_) {
                fprintf(stderr, "Node: %d: Value %ld at offset %ld/%ld\
                        greater than %lu\n", id_, *curHash, offset, 
                        curOffset_, separator_);
                assert(false);
            }
            advance(1, offset);
        }
        for (uint32_t i=0; i<children_.size(); i++) {
            if (!children_[i]->checkIntegrity())
                return false;
        }
#endif
        return true;
    }
}

