#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "CompressTree.h"
#include "CTNode.h"
#include "snappy.h"
#include "zlib.h"

namespace compresstree {
    Node::Node(NodeType typ, CompressTree* tree, bool alloc) :
        tree_(tree),
        typ_(typ),
        id_(CompressTree::nodeCtr++),
        parent_(NULL),
        numElements_(0),
        curOffset_(0),
        isCompressed_(false),
        compressible_(true),
        givenForComp_(false),
        cancelCompress_(false)
    {
        // Allocate memory for data buffer
        if (alloc) {
            assert(!posix_memalign((void**)&data_, sizeof(size_t),
                    BUFFER_SIZE));
        } else {
            data_ = NULL;
        }
        compressed_ = NULL;

        if (tree_->alg_ == SNAPPY) {
            compress = &Node::asyncCompress;
            decompress = &Node::snappyDecompress;
        } else if (tree_->alg_ == ZLIB) {
            compress = &Node::zlibCompress;
            decompress = &Node::zlibDecompress;
        } else {
            fprintf(stderr, "Compression algorithm\n");
            assert(false);
        }
        pthread_mutex_init(&gfcMutex_, NULL);
        pthread_cond_init(&gfcCond_, NULL);
    }

    Node::~Node()
    {
        if (data_) {
            free(data_);
            data_ = NULL;
        }
        if (compressed_) {
            free(compressed_);
            compressed_ = NULL;
        }
        pthread_mutex_destroy(&gfcMutex_);
        pthread_cond_destroy(&gfcCond_);
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

    bool Node::emptyBuffer(EmptyType type)
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
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\
                        %lu/%lu\n", id_, curOffset_, EMPTY_THRESHOLD);
#endif
            }
            return true;
        }

        if (curOffset_ == 0)
            goto emptyChildren;

        if (compressible_)
            CALL_MEM_FUNC(*this, decompress)();
        sortBuffer();
        // find the first separator strictly greater than the first element
        curHash = (uint64_t*)(data_ + offset);
        while (*curHash >= children_[curChild]->separator_) {
            curChild++;
            if (curChild >= children_.size()) {
                fprintf(stderr, "Node: %d: Can't place %lu among children\n", id_, *curHash);
                checkIntegrity();
                assert(false);
            }
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: first node chosen: %d (sep: %lu,\
            child: %d); first element: %lu\n", id_, children_[curChild]->id_,
            children_[curChild]->separator_, curChild, *curHash);
#endif
        while (offset < curOffset_) {
            curHash = (uint64_t*)(data_ + offset);

            if (offset >= curOffset_ || *curHash >= children_[curChild]->separator_) {
                // this separator is the largest separator that is not greater
                // than *curHash. This invariant needs to be maintained.
                if (numCopied > 0) { // at least one element for this
                    assert(CALL_MEM_FUNC(*children_[curChild], 
                            children_[curChild]->decompress)());
                    assert(children_[curChild]->copyIntoBuffer(data_ + 
                                lastOffset, offset - lastOffset));
                    children_[curChild]->addElements(numCopied);
        
                    assert(CALL_MEM_FUNC(*children_[curChild], 
                            children_[curChild]->compress)());
#ifdef CT_NODE_DEBUG
                    fprintf(stderr, "Copied %lu elements into node %d; child\
                            offset: %ld, sep: %lu/%lu, off:(%ld/%ld)\n", numCopied, 
                            children_[curChild]->id_,
                            children_[curChild]->curOffset_,
                            children_[curChild]->separator_,
                            UINT64_MAX,
                            offset, curOffset_);
#endif
                    lastOffset = offset;
                    numCopied = 0;
                }
                // skip past all separators not greater than *curHash
                while (*curHash >= children_[curChild]->separator_) {
                    curChild++;
                    if (curChild >= children_.size()) {
                        fprintf(stderr, "Can't place %lu among children\n", *curHash);
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
            CALL_MEM_FUNC(*children_[curChild], 
                    children_[curChild]->decompress)();
            assert(children_[curChild]->copyIntoBuffer(data_ + 
                        lastOffset, offset - lastOffset));
            children_[curChild]->addElements(numCopied);
            CALL_MEM_FUNC(*children_[curChild], 
                    children_[curChild]->compress)();
            
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Copied %lu elements into node %d; child\
                    offset: %ld, sep: %lu, off:(%ld/%ld)\n", numCopied, 
                    children_[curChild]->id_,
                    children_[curChild]->curOffset_,
                    children_[curChild]->separator_,
                    offset, curOffset_);
#endif
        }
        if (compressible_) {
            CALL_MEM_FUNC(*this, compress)();
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
                if (type == ASYNC_EMPTY)
                    tree_->addNodeToEmpty(children_[curChild]);
                else if (type == SYNC_EMPTY)
                    children_[curChild]->emptyBuffer(SYNC_EMPTY);
            }
        }

        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
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

    void Node::quicksort(uint64_t** arr, size_t uleft, size_t uright)
    {
        int64_t i, j, stack_pointer = -1;
        int64_t left = uleft;
        int64_t right = uright;
        int64_t* rstack = new int64_t[128];
        uint64_t *swap, *temp;
        while (true) {
            if (right - left <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = arr[j];
                    i = j - 1;
                    if (i < 0) {
                        fprintf(stderr, "Noo");
                        assert(false);
                    }
                    while (i >= left && (*arr[i] > *swap)) {
                        arr[i + 1] = arr[i--];
                    }
                    arr[i + 1] = swap;
                }
                if (stack_pointer == -1) {
                    break;
                }
                right = rstack[stack_pointer--];
                left = rstack[stack_pointer--];
            } else {
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;
                swap = arr[median]; arr[median] = arr[i]; arr[i] = swap;
                if (*arr[left] > *arr[right]) {
                    swap = arr[left]; arr[left] = arr[right]; arr[right] = swap;
                }
                if (*arr[i] > *arr[right]) {
                    swap = arr[i]; arr[i] = arr[right]; arr[right] = swap;
                }
                if (*arr[left] > *arr[i]) {
                    swap = arr[left]; arr[left] = arr[i]; arr[i] = swap;
                }
                temp = arr[i];
                while (true) {
                    while (*arr[++i] < *temp);
                    while (*arr[--j] > *temp);
                    if (j < i) {
                        break;
                    }
                    swap = arr[i]; arr[i] = arr[j]; arr[j] = swap;
                }
                arr[left + 1] = arr[j];
                arr[j] = temp;
                if (right - i + 1 >= j - left) {
                    rstack[++stack_pointer] = i;
                    rstack[++stack_pointer] = right;
                    right = j - 1;
                } else {
                    rstack[++stack_pointer] = left;
                    rstack[++stack_pointer] = j - 1;
                    left = i;
                }
            }
        } 
        delete[] rstack;
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
        if (isCompressed()) {
            fprintf(stderr, "Node %d is compressed still!\n", id_);
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
        PartialAgg *lastPAO, *thisPAO;
        tree_->createPAO_(NULL, &lastPAO);
        tree_->createPAO_(NULL, &thisPAO);
        for (uint64_t i=1; i<numElements_; i++) {
            if (*(tree_->els_[i]) == *(tree_->els_[lastIndex])) {
                CompressTree::actr++;
                // aggregate elements
                if (i == lastIndex + 1)
                    deserializePAO(tree_->els_[lastIndex], lastPAO);
                deserializePAO(tree_->els_[i], thisPAO);
                if (!strcmp(thisPAO->key, lastPAO->key)) {
                    lastPAO->merge(thisPAO);
                    CompressTree::cctr++;
                    continue;
                }
            }
            // copy hash and size into auxBuffer_
            if (i > lastIndex + 1) {
                lastPAO->serialize(tree_->serBuf_);
                buf_size = strlen(tree_->serBuf_);
                memmove(tree_->auxBuffer_ + auxOffset, 
                        (void*)(tree_->els_[lastIndex]), sizeof(uint64_t));
                auxOffset += sizeof(uint64_t);
                memmove(tree_->auxBuffer_ + auxOffset, 
                        (void*)(&buf_size), sizeof(size_t));
                auxOffset += sizeof(size_t);
                memmove(tree_->auxBuffer_ + auxOffset, 
                        (void*)(tree_->serBuf_), buf_size);
                auxOffset += buf_size;
                CompressTree::bctr++;
            } else {
                buf_size = *(size_t*)(tree_->els_[lastIndex] + 1);
                el_size = sizeof(uint64_t) + sizeof(size_t) + buf_size;
                memmove(tree_->auxBuffer_ + auxOffset, 
                        (void*)(tree_->els_[lastIndex]), el_size);
                auxOffset += el_size;
            }
            auxEls++;
            lastIndex = i;
        }
        buf_size = *(size_t*)(tree_->els_[lastIndex] + 1);
        el_size = sizeof(uint64_t) + sizeof(size_t) + buf_size;
        memmove(tree_->auxBuffer_ + auxOffset, 
                (void*)(tree_->els_[lastIndex]), el_size);
        auxOffset += el_size;
        auxEls++;

        tree_->destroyPAO_(lastPAO);
        tree_->destroyPAO_(thisPAO);

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
        if (!advance(numElements_/2, offset))
            return false;

        // select median value
        uint64_t* median_hash;
        size_t nextOffset = offset;
        advance(1, nextOffset);

        median_hash = (uint64_t*)(data_ + offset);
        size_t nj = 1;
        // TODO; Handle the case where all the elements until the end are same
        // although that is unlikely
        while (*(uint64_t*)(data_ + nextOffset) == *median_hash) {
            median_hash = (uint64_t*)(data_ + nextOffset);
            advance(1, nextOffset);
            nj++;
        }
        offset = nextOffset;
        median_hash = (uint64_t*)(data_ + offset);

        // check if we have reached limit of nodes that can be kept in-memory
        if (CompressTree::nodeCtr < tree_->nodesInMemory_) {
            // create new leaf
            Node* newLeaf = new Node(LEAF, tree_, true);
            newLeaf->copyIntoBuffer(data_ + offset, curOffset_ - offset);
            newLeaf->addElements(numElements_ - numElements_/2 - nj);
            newLeaf->separator_ = separator_;
            newLeaf->checkIntegrity();

            // set this leaf properties
            curOffset_ = offset;
            reduceElements(numElements_ - numElements_/2 - nj);
            separator_ = *median_hash;
            checkIntegrity();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node %d splits to Node %d: new offsets: %lu and\
                    %lu; new separators: %lu and %lu\n", id_, newLeaf->id_, 
                    curOffset_, newLeaf->curOffset_, separator_, newLeaf->separator_);
#endif

            // if leaf is also the root, create new root
            if (isRoot()) {
                setCompressible(true);
                tree_->createNewRoot(newLeaf);
            } else {
                parent_->addChild(newLeaf);
            }
            return newLeaf;
        } else {
            pthread_mutex_lock(&(tree_->evictedBufferMutex_));
            size_t wrtDataSize = curOffset_ - offset;
            if (wrtDataSize + tree_->evictedBufferOffset_ > BUFFER_SIZE) {
                fprintf(stderr, "We're losing data. Handle this case!\n");
                pthread_mutex_unlock(&(tree_->evictedBufferMutex_));
                return NULL;
            }
            memmove(tree_->evictedBuffer_ + tree_->evictedBufferOffset_, 
                    data_ + offset, wrtDataSize);
            tree_->evictedBufferOffset_ += wrtDataSize;
            curOffset_ = offset;
            reduceElements(numElements_ - numElements_/2);
            checkIntegrity();
            tree_->numEvicted_ = numElements_ - numElements_ / 2;
            pthread_mutex_unlock(&(tree_->evictedBufferMutex_));

            return NULL;
        }
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

    char* Node::getValue(uint64_t* hashPtr)
    {
        return (char*)((char*)hashPtr + sizeof(uint64_t) + sizeof(size_t));
    }

    void Node::deserializePAO(uint64_t* hashPtr, PartialAgg*& pao)
    {
        size_t buf_size = *(size_t*)(hashPtr + 1);
        memmove(tree_->serBuf_, (void*)getValue(hashPtr), buf_size);
        pao->deserialize(tree_->serBuf_);
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
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Node %d added at pos %u, [", id_, 
                newNode->id_, i);
        for (uint32_t j=0; j<children_.size(); j++)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "], num children: %ld\n", children_.size());
#endif
        // set parent
        newNode->parent_ = this;

        return true;
    }

    bool Node::splitNonLeaf()
    {
        // ensure node's buffer is empty
#ifdef ENABLE_ASSERT_CHECKS
        if (curOffset_ > 0) {
            fprintf(stderr, "Node %d has non-empty buffer\n", id_);
            assert(false);
        }
#endif
        // create new node
        Node* newNode = new Node(NON_LEAF, tree_, false);
        // move the last floor((b+1)/2) children to new node
        int newNodeChildIndex = children_.size()-(tree_->b_+1)/2;
        assert(children_[newNodeChildIndex]->separator_ > children_[newNodeChildIndex-1]->separator_);
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
#ifdef CT_NODE_DEBUG
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
        // compress new node
        CALL_MEM_FUNC(*newNode, newNode->compress)();

        if (isRoot()) {
            setCompressible(true);
            free(data_);
            data_ = NULL;
            // actually compress the node that was formerly the root
            isCompressed_ = false;
            CALL_MEM_FUNC(*this, compress)();
            return tree_->createNewRoot(newNode);
        } else
            return parent_->addChild(newNode);
    }

    bool Node::advance(size_t n, size_t& offset)
    {
        size_t* bufSize;
        if (offset >= curOffset_) {
            fprintf(stderr, "off: %lu, curoff: %lu\n", offset, curOffset_);
            assert(false);
        }
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

    size_t Node::getNumSiblings()
    {
        if (isRoot())
            return 1;
        else
            return parent_->children_.size();
    }
    
    bool Node::asyncCompress()
    {
        pthread_mutex_lock(&gfcMutex_);
        // check if node already in to-compress list
        if (givenForComp_) {
            // check if compression request has been cancelled
            if (cancelCompress_) {
                /* reset cancellation request; node need not be added
                 * again */
                cancelCompress_ = false;
                pthread_mutex_unlock(&gfcMutex_);
                return true;
            } else { // if not cancelled, we're compressing twice
                fprintf(stderr, "Trying to compress node %d twice", id_);
                assert(false);
            }
        } else {
            givenForComp_ = true;
        }
        pthread_mutex_unlock(&gfcMutex_);
        pthread_mutex_lock(&(tree_->nodesReadyForCompressMutex_));
        tree_->nodesToCompress_.push_back(this);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "call for compressing node %d\t", id_);
        for (int i=0; i<tree_->nodesToCompress_.size(); i++)
            fprintf(stderr, "%d, ", tree_->nodesToCompress_[i]->id_);
        fprintf(stderr, "\n");
#endif
        pthread_cond_signal(&(tree_->nodesReadyForCompression_));
        pthread_mutex_unlock(&(tree_->nodesReadyForCompressMutex_));
        return true;
    }

    bool Node::snappyCompress()
    {
        if (!compressible_ ) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "root not compressible\t");
            for (int i=0; i<tree_->nodesToCompress_.size(); i++)
                fprintf(stderr, "%d, ", tree_->nodesToCompress_[i]->id_);
            fprintf(stderr, "\n");
#endif
            return true;
        }
#ifdef ENABLE_ASSERT_CHECKS
        if (isCompressed()) {
            fprintf(stderr, "Node %d already compressed\n", id_);
            assert(false);
        }
#endif
        if (data_ != NULL) {
            size_t compressed_length;
            snappy::RawCompress(data_, curOffset_, tree_->compBuffer_,
                    &compressed_length);
            assert(!posix_memalign((void**)&compressed_, sizeof(size_t), compressed_length));
            memmove(compressed_, tree_->compBuffer_, compressed_length);
            /* Can be called from multiple places:
             * + emptyBuffer()-1: on the node whose buffer was emptied - no free
             * + emptyBuffer()-2: on children of emptied node - free required
             * + splitNonLeaf(): called on ex-root node which is empty - no free
             * + handleFullLeaves(): called on split leaves - free required
             */
            compLength_ = compressed_length;
            free(data_);
            data_ = NULL;
        } else { // data_ is NULL
            compressed_ = NULL;
        }

        isCompressed_ = true;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "compressed node %d\t", id_);
        for (int i=0; i<tree_->nodesToCompress_.size(); i++)
            fprintf(stderr, "%d, ", tree_->nodesToCompress_[i]->id_);
        fprintf(stderr, "\n");
#endif
        return true;
    }

    bool Node::snappyDecompress()
    {
        if (!compressible_)
            return true;

        // check if node is queued up for compression
        pthread_mutex_lock(&gfcMutex_);
        // since we've got the lock, the compression can't be in-progress
        if (givenForComp_) {
            // this means that this node is still queued up, so we cancel
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node %d: compression cancelled\n", id_);
#endif
            cancelCompress_ = true;
            if (data_ == NULL) {
                assert(!posix_memalign((void**)&data_, sizeof(size_t), BUFFER_SIZE));
            }
            pthread_mutex_unlock(&gfcMutex_);
            return true;
        } else
            pthread_mutex_unlock(&gfcMutex_);

#ifdef ENABLE_ASSERT_CHECKS
        if (!isCompressed() && !cancelCompress_) {
            fprintf(stderr, "Node %d already decompressed\n", id_);
            assert(false);
        }
#endif
        assert(!posix_memalign((void**)&data_, sizeof(size_t), BUFFER_SIZE));

        /* + emptyBuffer()-1: the node to be emptied is not NULL
         * + emptyBuffer()-2: children may be empty
         * + handleFullLeaves(): not empty
         */
        if (compressed_ != NULL) {
            snappy::RawUncompress(compressed_, compLength_, data_);
            free(compressed_);
            compressed_ = NULL;
        }
        isCompressed_ = false;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "decompressed node %d\n", id_);
#endif
        return true;
    }

    bool Node::zlibCompress()
    {
        int ret;
        z_stream strm;
        if (!compressible_)
            return true;
        /* allocate deflate state */
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
        assert(ret == Z_OK);

        /* set input/output buffer */
        strm.avail_in = curOffset_;
        strm.next_in = (unsigned char*)data_;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out = (unsigned char*)tree_->compBuffer_;

        /* compress */
        ret = deflate(&strm, Z_FINISH);
        assert(strm.avail_in == 0);
        compLength_ = BUFFER_SIZE - strm.avail_out;
        (void)deflateEnd(&strm);

        free(data_);
        char* compressed = (char*)malloc(compLength_);
        memmove(compressed, tree_->compBuffer_, compLength_);
        data_ = compressed;
        isCompressed_ = true;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "len: %ld, comp len: %ld\n", 
                curOffset_, compLength_);
#endif
        return true;
    }

    bool Node::zlibDecompress()
    {
        if (!compressible_)
            return true;

        char* buf = (char*)malloc(BUFFER_SIZE);

        if (data_ != NULL) {
            int ret;
            z_stream strm;
            strm.zalloc = Z_NULL;
            strm.zfree = Z_NULL;
            strm.opaque = Z_NULL;
            strm.avail_in = 0;
            strm.next_in = NULL;
            ret = inflateInit(&strm);
            assert(ret == Z_OK);

            /* set input/output buffers */
            strm.avail_in = compLength_;
            strm.next_in = (unsigned char*)data_;
            strm.avail_out = BUFFER_SIZE;
            strm.next_out = (unsigned char*)buf;

            /* do decompression */
            ret = inflate(&strm, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);
#ifdef ENABLE_ASSERT_CHECKS
            size_t uncomp_length = BUFFER_SIZE - strm.avail_out;
            assert(curOffset_ == uncomp_length);
#endif
            (void)inflateEnd(&strm);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "comp len: %ld, len: %ld\n", 
                    compLength_, curOffset_);
#endif
            free(data_);
        }
        data_ = buf;
        isCompressed_ = false;
        return true;
    }

    bool Node::isCompressed()
    {
        return isCompressed_;
    }

    void Node::setCompressible(bool flag)
    {
        compressible_ = flag;
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
                fprintf(stderr, "Node: %d: Value %lu at offset %ld/%ld\
                        greater than %lu\n", id_, *curHash, offset, 
                        curOffset_, separator_);
                assert(false);
            }
            advance(1, offset);
        }
        Node* parentNode = parent_;
        uint64_t lastHash = *curHash;
        while (parentNode != NULL) {
            for (uint32_t i=0; i<parentNode->children_.size(); i++) {
                if (parentNode->children_[i] == this) {
                    assert(lastHash < parentNode->children_[i]->separator_);
                    lastHash = parentNode->children_[i]->separator_;
                }
            }
            parentNode = parentNode->parent_;
        }
        for (int32_t i=1; i<children_.size(); i++) {
            if (children_[i-1]->separator_ >= children_[i]->separator_) {
                fprintf(stderr, "Node: %d: Child %d has separator %lu\
                        greater than child %d's %lu\n", id_, i-1, 
                        children_[i-1]->separator_, i, children_[i]->separator_);
                assert(false);
            }
        }
        for (int32_t i=0; i<children_.size(); i++) {
            if (!children_[i]->checkIntegrity())
                return false;
        }
#endif
        return true;
    }

    bool Node::parseNode()
    {
#ifdef ENABLE_INTEGRITY_CHECK
        size_t* bufSize;
        size_t offset;
        for (uint32_t i=0; i<numElements_; i++) {
            offset += sizeof(uint64_t);
            bufSize = (size_t*)(data_ + offset);
            offset += sizeof(size_t) + *bufSize;
            if (*bufSize == 0) {
                fprintf(stderr, "%lu, bufsize: %lu, %ld\n", offset, *bufSize, *(size_t*)(data_ + offset + 16));
                assert(false);
            }
        }
#endif
        return true;
    }
}

