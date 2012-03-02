#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "CompressTree.h"
#include "CTNode.h"
#include "Slaves.h"
#include "snappy.h"
#include "zlib.h"

namespace compresstree {
    Node::EmptyType Node::emptyType_ = IF_FULL;


    Node::Buffer::Buffer()
    {
        allocate();
    }

    Node::Buffer::~Buffer()
    {
        deallocate();
    }

    void Node::Buffer::allocate()
    {
        hashes_ = (uint32_t*)malloc(sizeof(uint32_t) * 
                compresstree::MAX_ELS_PER_BUFFER);
        sizes_ = (uint32_t*)malloc(sizeof(uint32_t) *
                compresstree::MAX_ELS_PER_BUFFER);
        data_ = (char*)malloc(BUFFER_SIZE);
    }

    void Node::Buffer::deallocate()
    {
        if (hashes_) { free(hashes_); hashes_ = NULL;}
        if (sizes_) { free(sizes_); sizes_ = NULL;}
        if (data_) { free(data_); data_ = NULL;}
    }

    inline bool Node::Buffer::empty()
    {
        return (data_ == NULL);
    }

    Node::Node(CompressTree* tree, uint32_t level) :
        tree_(tree),
        level_(level),
        parent_(NULL),
        numElements_(0),
        curOffset_(0),
        state_(DECOMPRESSED),
        compressible_(true),
        queuedForEmptying_(false),
        queuedForCompAct_(false),
        compAct_(NONE)
    {
        id_ = tree_->nodeCtr++;

        if (tree_->alg_ == SNAPPY) {
            compress = &Node::asyncCompress;
            decompress = &Node::asyncDecompress;
        } else {
            fprintf(stderr, "Compression algorithm\n");
            assert(false);
        }
        pthread_mutex_init(&stateMutex_, NULL);
        pthread_mutex_init(&queuedForEmptyMutex_, NULL);
        pthread_mutex_init(&compActMutex_, NULL);
        pthread_cond_init(&compActCond_, NULL);
        tree_->createPAO_(NULL, &lastPAO);
        tree_->createPAO_(NULL, &thisPAO);

#ifdef ENABLE_PAGING
        pthread_mutex_init(&pageMutex_, NULL);
        pthread_cond_init(&pageCond_, NULL);

        pageable_ = true;
        queuedForPaging_ = false;
        pageAct_ = NO_PAGE;
        char* fileName = (char*)malloc(100);
        char* nodeNum = (char*)malloc(10);
        strcpy(fileName, "/mnt/hamur/minni_data/");
        sprintf(nodeNum, "%d", id_);
        strcat(fileName, nodeNum);
        strcat(fileName, ".buf");
        fd_ = open(fileName, O_CREAT|O_RDWR|O_TRUNC, 0755);
        free(fileName);
        free(nodeNum);
#endif
#ifdef ENABLE_COUNTERS
        if (tree_->monitor_)
            tree_->monitor_->decompCtr++;
#endif
    }

    Node::~Node()
    {
        pthread_mutex_destroy(&stateMutex_);
        pthread_mutex_destroy(&queuedForEmptyMutex_);
        pthread_mutex_destroy(&compActMutex_);
        pthread_cond_destroy(&compActCond_);

        tree_->destroyPAO_(lastPAO);
        tree_->destroyPAO_(thisPAO);
#ifdef ENABLE_PAGING
        pthread_mutex_destroy(&pageMutex_);
        pthread_cond_destroy(&pageCond_);

        close(fd_);
#endif
    }

    bool Node::insert(uint64_t hash, const std::string& value)
    {
        // check if buffer is compressed
        if (isCompressed())
            return false;

        uint32_t hashv = (uint32_t)hash;
        uint32_t buf_size = value.size();

        // copy into Buffer fields
        buffer_.hashes_[numElements_] = hashv;
        buffer_.sizes_[numElements_] = buf_size;
        memmove(buffer_.data_ + curOffset_, (void*)value.data(), buf_size);
        curOffset_ += buf_size;

        numElements_++;
        return true;
    }

    bool Node::isLeaf() const
    {
        if (children_.size() == 0)
            return true;
        return false;
    }

    bool Node::isRoot() const
    {
        if (parent_ == NULL)
            return true;
        return false;
    }

    bool Node::emptyOrCompress()
    {
        if (emptyType_ == ALWAYS || isFull()) {
            tree_->sorter_->addNode(this);
            tree_->sorter_->wakeup();
        } else {
            CALL_MEM_FUNC(*this, compress)();
        }
    }

    bool Node::emptyBuffer()
    {
        uint32_t curChild = 0;
        // offset till which elements have been written
        uint32_t offset = 0;
        uint32_t curElement = 0;
        uint32_t lastElement = 0; 
        uint32_t numCopied = 0;

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
            } else { // we aggregate and compress
                aggregateBuffer();
                CALL_MEM_FUNC(*this, compress)();
            }
            return true;
        }

        if (curOffset_ == 0) {
            for (curChild=0; curChild < children_.size(); curChild++) {
                children_[curChild]->emptyOrCompress();
            }
            goto checkSplitNonLeaf;
        }
            

        aggregateBuffer();
        // find the first separator strictly greater than the first element
        while (buffer_.hashes_[curElement] >= 
                children_[curChild]->separator_) {
            children_[curChild]->emptyOrCompress();
            curChild++;
#ifdef ENABLE_ASSERT_CHECKS
            if (curChild >= children_.size()) {
                fprintf(stderr, "Node: %d: Can't place %u among children\n", id_, *curHash);
                checkIntegrity();
                assert(false);
            }
#endif
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: first node chosen: %d (sep: %u,\
            child: %d); first element: %u\n", id_, children_[curChild]->id_,
            children_[curChild]->separator_, curChild, buffer_.hashes_[0]);
#endif
        while (curElement < numElements_) {
            if (buffer_.hashes_[curElement] >= 
                    children_[curChild]->separator_) {
                /* this separator is the largest separator that is not greater
                 * than *curHash. This invariant needs to be maintained.
                 */
                if (curElement > lastElement) {
                    children_[curChild]->waitForCompressAction(DECOMPRESS);
#ifdef ENABLE_ASSERT_CHECKS
                    if (children_[curChild]->isCompressed()) {
                        fprintf(stderr, "Node %d should be decompressed\n", children_[curChild]->id_);
                        assert(false);
                    }
#endif
                    // if child is empty, allocate buffers
                    if (children_[curChild]->buffer_.empty()) {
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "Node: %d: alloc'ing fresh buffers\n",
                                children_[curChild]->id_);
#endif
                        children_[curChild]->buffer_.allocate();
                    }

                    // copy elements into child
                    children_[curChild]->copyIntoBuffer(*this, lastElement,
                                curElement - lastElement);
#ifdef CT_NODE_DEBUG
                    fprintf(stderr, "Copied %lu elements into node %d; child\
                            offset: %ld, sep: %lu/%lu\n",
                            curElement - lastElement,
                            children_[curChild]->id_,
                            children_[curChild]->curOffset_,
                            children_[curChild]->separator_);
#endif
                    lastElement = curElement;
                }
                // skip past all separators not greater than the current hash
                while (buffer_.hashes_[curElement] 
                        >= children_[curChild]->separator_) {
                    children_[curChild]->emptyOrCompress();
                    curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                    if (curChild >= children_.size()) {
                        fprintf(stderr, "Can't place %u among children\n", *curHash);
                        assert(false);
                    }
#endif
                }
            }
            // proceed to next element
            curElement++;
        }

        // copy remaining elements into child
        if (curElement >= lastElement) {
            children_[curChild]->waitForCompressAction(DECOMPRESS);
#ifdef ENABLE_ASSERT_CHECKS
            if (children_[curChild]->isCompressed()) {
                fprintf(stderr, "Node %d should be decompressed\n", children_[curChild]->id_);
                assert(false);
            }
#endif

            // if child is empty, allocate buffers
            if (children_[curChild]->buffer_.empty()) {
                children_[curChild]->buffer_.allocate();
            }

            // copy elements into child
            children_[curChild]->copyIntoBuffer(*this, lastElement,
                        curElement - lastElement);
            children_[curChild]->emptyOrCompress();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Copied %lu elements into node %d; child\
                    offset: %ld, sep: %lu\n",
                    curElement - lastElement,
                    children_[curChild]->id_,
                    children_[curChild]->curOffset_,
                    children_[curChild]->separator_);
#endif
        }

        // reset
        curOffset_ = 0;
        numElements_ = 0;

        if (!isRoot()) {
            buffer_.deallocate();
            CALL_MEM_FUNC(*this, compress)();
        }

checkSplitNonLeaf:
        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
        return true;
    }

    void Node::quicksort(size_t uleft, size_t uright)
    {
        int32_t i, j, stack_pointer = -1;
        int32_t left = uleft;
        int32_t right = uright;
        int32_t* rstack = new int32_t[128];
        uint32_t swap, temp;
        char *swap_perm, *temp_perm;
        uint32_t swap_size, temp_size;

        uint32_t* arr = buffer_.hashes_;
        uint32_t* siz = buffer_.sizes_;
        while (true) {
            if (right - left <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = arr[j];
                    swap_size = siz[j];
                    swap_perm = perm_[j];

                    i = j - 1;
                    if (i < 0) {
                        fprintf(stderr, "Noo");
                        assert(false);
                    }
                    while (i >= left && (arr[i] > swap)) {
                        arr[i + 1] = arr[i];
                        siz[i + 1] = siz[i];
                        perm_[i + 1] = perm_[i];
                        i--;
                    }
                    arr[i + 1] = swap;
                    siz[i + 1] = swap_size;
                    perm_[i + 1] = swap_perm;
                }
                if (stack_pointer == -1) {
                    break;
                }
                right = rstack[stack_pointer];
                left = rstack[stack_pointer];
                stack_pointer--;
            } else {
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;
                swap = arr[median]; arr[median] = arr[i]; arr[i] = swap;
                swap_size = siz[median]; siz[median] = siz[i]; 
                        siz[i] = swap_size;
                swap_perm = perm_[median]; perm_[median] = perm_[i]; 
                        perm_[i] = swap_perm;
                if (arr[left] > arr[right]) {
                    swap = arr[left]; arr[left] = arr[right]; arr[right] = swap;
                    swap_size = siz[left]; siz[left] = siz[right]; 
                            siz[right] = swap_size;
                    swap_perm = perm_[left]; perm_[left] = perm_[right]; 
                            perm_[right] = swap_perm;
                }
                if (arr[i] > arr[right]) {
                    swap = arr[i]; arr[i] = arr[right]; arr[right] = swap;
                    swap_size = siz[i]; siz[i] = siz[right]; 
                            siz[right] = swap_size;
                    swap_perm = perm_[i]; perm_[i] = perm_[right]; 
                            perm_[right] = swap_perm;
                }
                if (arr[left] > arr[i]) {
                    swap = arr[left]; arr[left] = arr[i]; arr[i] = swap;
                    swap_size = siz[left]; siz[left] = siz[i];
                            siz[i] = swap_size;
                    swap_perm = perm_[left]; perm_[left] = perm_[i]; 
                            perm_[i] = swap_perm;
                }
                temp = arr[i];
                temp_size = siz[i];
                temp_perm = perm_[i];
                while (true) {
                    while (arr[++i] < temp);
                    while (arr[--j] > temp);
                    if (j < i) {
                        break;
                    }
                    swap = arr[i]; arr[i] = arr[j]; arr[j] = swap;
                    swap_size = siz[i]; siz[i] = siz[j];
                            siz[j] = swap_size;
                    swap_perm = perm_[i]; perm_[i] = perm_[j];
                            perm_[j] = swap_perm;
                }
                arr[left + 1] = arr[j];
                siz[left + 1] = siz[j];
                perm_[left + 1] = perm_[j];
                arr[j] = temp;
                siz[j] = temp_size;
                perm_[j] = temp_perm;
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
        if (numElements_ == 0)
            return true;
#ifdef ENABLE_ASSERT_CHECKS
        if (isCompressed()) {
            fprintf(stderr, "Node %d is not decompressed still!\n", id_);
            assert(false);
        }
#endif
        // initialize pointers to serialized PAOs
        perm_ = (char**)malloc(sizeof(char*) * numElements_);
        size_t offset = 0;
        for (uint32_t i=0; i<numElements_; i++) {
            perm_[i] = buffer_.data_ + offset;
            offset += buffer_.sizes_[i];
        }

        // quicksort elements
        quicksort(0, numElements_ - 1);
    }

    bool Node::aggregateBuffer()
    {
        uint32_t el_size;
        size_t auxOffset = 0;
        size_t auxEls = 0;
        uint32_t buf_size;

        // aggregate elements in buffer
        uint32_t lastIndex = 0;
        for (uint32_t i=1; i<numElements_; i++) {
            if (buffer_.hashes_[i] == buffer_.hashes_[lastIndex]) {
#ifdef ENABLE_COUNTERS
                tree_->monitor_->actr++;
#endif
                // aggregate elements
                if (i == lastIndex + 1) {
                    assert(lastPAO->deserialize(perm_[lastIndex], 
                            buffer_.sizes_[i]));
                }
                assert(thisPAO->deserialize(perm_[i], buffer_.sizes_[i]));
                if (!thisPAO->key().compare(lastPAO->key())) {
                    lastPAO->merge(thisPAO);
#ifdef ENABLE_COUNTERS
                    tree_->monitor_->cctr++;
#endif
                    continue;
                }
            }
            // copy hash and size into auxBuffer_
            if (i == lastIndex + 1) {
                tree_->auxBuffer_.hashes_[auxEls] = buffer_.hashes_[lastIndex];
                // the size wouldn't have changed
                tree_->auxBuffer_.sizes_[auxEls] = buffer_.sizes_[lastIndex];
                memmove(tree_->auxBuffer_.data_ + auxOffset,
                        (void*)(perm_[lastIndex]),
                        buffer_.sizes_[lastIndex]);
                auxOffset += el_size;
            } else {
                std::string serialized;
                lastPAO->serialize(&serialized);
                uint32_t buf_size = serialized.size();

                tree_->auxBuffer_.hashes_[auxEls] = buffer_.hashes_[lastIndex];
                tree_->auxBuffer_.sizes_[auxEls] = buf_size;
                memmove(tree_->auxBuffer_.data_ + auxOffset,
                        (void*)(perm_[lastIndex]), buf_size);                        
                auxOffset += buf_size;
#ifdef ENABLE_COUNTERS
                tree_->monitor_->bctr++;
#endif
            }
            auxEls++;
            lastIndex = i;
        }
        // copy the last PAO
        std::string serialized;
        lastPAO->serialize(&serialized);
        buf_size = serialized.size();

        tree_->auxBuffer_.hashes_[auxEls] = buffer_.hashes_[lastIndex];
        tree_->auxBuffer_.sizes_[auxEls] = buf_size;
        memmove(tree_->auxBuffer_.data_ + auxOffset,
                (void*)(perm_[lastIndex]), buf_size);                        
        auxOffset += buf_size;
        auxEls++;

        // free pointer memory
        free(perm_);

        // swap buffer pointers
        uint32_t* temp_hashes = buffer_.hashes_;
        uint32_t* temp_sizes = buffer_.sizes_;
        char* temp_data = buffer_.data_;

        buffer_.hashes_ = tree_->auxBuffer_.hashes_;
        buffer_.sizes_ = tree_->auxBuffer_.sizes_;
        buffer_.data_ = tree_->auxBuffer_.data_;

        tree_->auxBuffer_.hashes_ = temp_hashes;
        tree_->auxBuffer_.sizes_ = temp_sizes;
        tree_->auxBuffer_.data_ = temp_data;

        curOffset_ = auxOffset;
        numElements_ = auxEls;

        return true;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::splitLeaf()
    {
        checkIntegrity();

        // select splitting index
        uint32_t splitIndex = numElements_/2;
        while (buffer_.hashes_[splitIndex] == 
                buffer_.hashes_[splitIndex-1]) {
            splitIndex++;
#ifdef ENABLE_ASSERT_CHECKS
            if (splitIndex == numElements_)
                assert(false);
            }                
#endif
        }

        // create new leaf
        Node* newLeaf = new Node(tree_, 0);
        newLeaf->copyIntoBuffer(*this, splitIndex, numElements_ - splitIndex);

        // modify this leaf properties
        size_t offset = 0;
        for (uint32_t i=0; i<splitIndex; i++) {
            offset += buffer_.sizes_[i];
        }
        curOffset_ = offset;
        numElements_ = splitIndex;
        separator_ = buffer_.hashes_[splitIndex];

        // check integrity of both leaves
        newLeaf->checkIntegrity();
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
    }

    bool Node::copyIntoBuffer(const Node& node, size_t index, size_t num)
    {
        // calculate offset
        size_t offset = 0;
        size_t num_bytes = 0;
        for (uint32_t i=0; i<index; i++) {
            offset += buffer_.sizes_[i];
        }
        for (uint32_t i=index; i<num; i++) {
            num_bytes += buffer_.sizes_[i];
        }
#ifdef ENABLE_ASSERT_CHECKS
        if (curOffset_ + num_bytes >= BUFFER_SIZE) {
            fprintf(stderr, "Node: %d, cOffset: %ld, buf: %ld\n", id_, 
                    curOffset_, buf_size); 
            assert(false);
        }
        if (buffer_.data_ == NULL) {
            fprintf(stderr, "data buffer is null\n");
            assert(false);
        }
#endif
        memmove(buffer_.hashes_ + numElements_, node.buffer_.hashes_ + index,
                num * sizeof(uint32_t));
        memmove(buffer_.sizes_ + numElements_, node.buffer_.sizes_ + index,
                num * sizeof(uint32_t));
        offset = 0;
        memmove(buffer_.data_ + curOffset_, node.buffer_.data_ + offset, 
                num_bytes);

        curOffset_ += num_bytes;
        numElements_ += num;
        separator_ = node.separator_;
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
        Node* newNode = new Node(tree_, level_);
        // move the last floor((b+1)/2) children to new node
        int newNodeChildIndex = children_.size()-(tree_->b_+1)/2;
#ifdef ENABLE_ASSERT_CHECKS
        if (children_[newNodeChildIndex]->separator_ <= 
                children_[newNodeChildIndex-1]->separator_) {
            fprintf(stderr, "%d sep is %lu and %d sep is %lu\n", 
                    newNodeChildIndex, 
                    children_[newNodeChildIndex]->separator_, 
                    newNodeChildIndex-1,
                    children_[newNodeChildIndex-1]->separator_);
            assert(false);
        }
#endif
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

        CALL_MEM_FUNC(*newNode, newNode->compress)();
        if (isRoot()) {
            setCompressible(true);
            buffer_.deallocate();
            // actually compress the node that was formerly the root
            setState(DECOMPRESSED);
            CALL_MEM_FUNC(*this, compress)();
            return tree_->createNewRoot(newNode);
        } else
            return parent_->addChild(newNode);
    }

    bool Node::isFull() const
    {
        if (curOffset_ > EMPTY_THRESHOLD)
            return true;
        return false;
    }

    uint32_t Node::level() const
    {
        return level_;
    }

    uint32_t Node::id() const
    {
        return id_;
    }

    inline void Node::setState(NodeState state)
    {
        pthread_mutex_lock(&stateMutex_);
        state_ = state;
        pthread_mutex_unlock(&stateMutex_);
    }
    
    bool Node::asyncCompress()
    {
        if (!compressible_)
            return true;
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
            if (curOffset_ == 0) {
                setState(COMPRESSED);
                queuedForCompAct_ = false;
                pthread_mutex_unlock(&compActMutex_);
                return true;
            } else {
                queuedForCompAct_ = true;
                compAct_ = COMPRESS;
            }
        }
        pthread_mutex_unlock(&compActMutex_);
        tree_->compressor_->addNode(this);
        tree_->compressor_->wakeup();
        return true;
    }
    
    bool Node::asyncDecompress()
    {
        if (!compressible_)
            return true;
        pthread_mutex_lock(&compActMutex_);
        // check if node already in list
        if (queuedForCompAct_) {
            // check if compression request is outstanding
            if (compAct_ == COMPRESS) {
                /* reset action request; node need not be added
                 * again */
                compAct_ = NONE;
                pthread_mutex_unlock(&compActMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d compression cancelled\n", id_);
#endif
                return true;
            } else if (compAct_ == NONE) {
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
            if (curOffset_ == 0) {
                setState(DECOMPRESSED);
                queuedForCompAct_ = false;
                pthread_mutex_unlock(&compActMutex_);
                return true;
            } else {
                queuedForCompAct_ = true;
                compAct_ = DECOMPRESS;
            }
        }
        pthread_mutex_unlock(&compActMutex_);
        tree_->compressor_->addNode(this);
        tree_->compressor_->wakeup();
        return true;
    }

    void Node::waitForCompressAction(const CompressionAction& act)
    {
        // make sure the buffer has been decompressed
        pthread_mutex_lock(&compActMutex_);
        while (queuedForCompAct_ && compAct_ == act)
            pthread_cond_wait(&compActCond_, &compActMutex_);
        pthread_mutex_unlock(&compActMutex_);
    }

    bool Node::snappyCompress()
    {
#ifdef ENABLE_ASSERT_CHECKS
        if (isCompressed()) {
            fprintf(stderr, "Node %d already compressed\n", id_);
            assert(false);
        }
#endif
        if (!buffer_.empty()) {
            // allocate memory for compressed buffers
            if (compressed_.empty())
                compressed_.allocate();

            snappy::RawCompress((const char*)buffer_.hashes_, 
                    numElements_ * sizeof(uint32_t), 
                    (char*)tree_->compBuffer_.hashes_,
                    (size_t*)&compLength_.hashLen);
            memmove(compressed_.hashes_, tree_->compBuffer_.hashes_, 
                    compLength_.hashLen);

            snappy::RawCompress((const char*)buffer_.sizes_, 
                    numElements_ * sizeof(uint32_t), 
                    (char*)tree_->compBuffer_.sizes_,
                    (size_t*)&compLength_.sizeLen);
            memmove(compressed_.sizes_, tree_->compBuffer_.sizes_, 
                    compLength_.sizeLen);

            snappy::RawCompress(buffer_.data_, curOffset_, 
                    tree_->compBuffer_.data_, 
                    (size_t*)&compLength_.dataLen);
            memmove(compressed_.data_, tree_->compBuffer_.data_, 
                    compLength_.dataLen);
            /* Can be called from multiple places:
             * + emptyBuffer()-1: on the node whose buffer was emptied - no free
             * + emptyBuffer()-2: on children of emptied node - free required
             * + splitNonLeaf(): called on ex-root node which is empty - no free
             * + handleFullLeaves(): called on split leaves - free required
             */
            buffer_.deallocate();
        } else {
            if (!compressed_.empty())
                compressed_.deallocate();
        }

        setState(COMPRESSED);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "compressed node %d\n", id_);
#endif
        return true;
    }

    bool Node::snappyDecompress()
    {
#ifdef ENABLE_ASSERT_CHECKS
        if (!isCompressed()) {
            fprintf(stderr, "Node %d already decompressed\n", id_);
            assert(false);
        }
#endif
        /* + emptyBuffer()-1: the node to be emptied is not NULL
         * + emptyBuffer()-2: children may be empty
         * + handleFullLeaves(): not empty
         */
        if (!compressed_.empty()) {
            buffer_.allocate();
            snappy::RawUncompress((const char*)compressed_.hashes_, 
                    compLength_.hashLen,
                    (char*)buffer_.hashes_);
            snappy::RawUncompress((const char*)compressed_.sizes_, 
                    compLength_.sizeLen,
                    (char*)buffer_.sizes_);
            snappy::RawUncompress(compressed_.data_, compLength_.dataLen,
                    buffer_.data_);
            compressed_.deallocate();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "decompressed node %d\n", id_);
#endif
        }
        setState(DECOMPRESSED);
        return true;
    }

    bool Node::isCompressed()
    {
        pthread_mutex_lock(&stateMutex_);
        if (state_ == COMPRESSED || state_ == PAGED_OUT) {
            pthread_mutex_unlock(&stateMutex_);
            return true;
        }
        pthread_mutex_unlock(&stateMutex_);
        return false;
    }

    void Node::setCompressible(bool flag)
    {
        compressible_ = flag;
    }

#ifdef ENABLE_PAGING
    bool Node::waitForPageIn()
    {
        // make sure the buffer has been page in
        pthread_mutex_lock(&pageMutex_);
        while (queuedForPaging_ && pageAct_ == PAGE_IN)
            pthread_cond_wait(&pageCond_, &pageMutex_);
        pthread_mutex_unlock(&pageMutex_);
    }

    bool Node::isPagedOut()
    {
        pthread_mutex_lock(&stateMutex_);
        if (state_ == PAGED_OUT) {
            pthread_mutex_unlock(&stateMutex_);
            return true;
        }
        pthread_mutex_unlock(&stateMutex_);
        return false;
    }

    bool Node::pageOut()
    {
        if (compLength_ > 0) {
            size_t ret;
            ret = pwrite(fd_, compressed_, compLength_, 0);
#ifdef ENABLE_ASSERT_CHECKS
            if (ret != compLength_) {
                fprintf(stderr, "Node %d page-out fail! Error: %d\n", id_, errno);
                fprintf(stderr, "written: %ld actual: %ld\n", ret, compLength_);
                assert(false);
            }
#endif
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node: %d: Flushed %ld bytes\n", id_, ret); 
#endif
            free(compressed_);
        }
        compressed_ = NULL;

        setState(PAGED_OUT);
        return true;
    }

    bool Node::pageIn()
    {
        if (compLength_ > 0) {
            compressed_ = (char*)malloc(BUFFER_SIZE);
            size_t ret = pread(fd_, compressed_, compLength_, 0);
#ifdef ENABLE_ASSERT_CHECKS
            if (ret != compLength_) {
                fprintf(stderr, "Node %d page-in fail! Error: %d\n", id_, errno);
                fprintf(stderr, "written: %ld actual: %ld\n", ret, compLength_);
                assert(false);
            }
#endif
        }

        setState(COMPRESSED);
        // set file pointer to beginning of file again
        lseek(fd_, 0, SEEK_SET);
        return true;
    }

    bool Node::isPinned() const
    {
        if (isRoot() || parent_->isRoot())
            return true;
        return false;
    }
#endif //ENABLE_PAGING

    bool Node::checkIntegrity()
    {
#ifdef ENABLE_INTEGRITY_CHECK
        size_t offset;
        uint32_t* curHash;
        offset = 0;
        for (size_t i=0; i<numElements_-1; i++) {
            if (buffer_.hashes_[i] > buffer_.hashes_[i+1])
                fprintf(stderr, "Node: %d: Hash %u at index %ld\
                        greater than hash %u at %ld\n", id_, 
                        buffer_.hashes_[i], i, buffer_.hashes_[i+1],
                        i+1);
                assert(false);
            }
        }
        if (buffer_.hashes_[numElements_-1] >= separator_) {
            fprintf(stderr, "Node: %d: Hash %u at index %ld\
                greater than separator %u\n", id_, 
                buffer_.hashes_[numElements_-1], numElements_-1,
                separator_);
            assert(false);
        }
#endif
        return true;
    }

    bool Node::parseNode()
    {
#ifdef ENABLE_INTEGRITY_CHECK
        uint32_t* bufSize;
        size_t offset;
        for (uint32_t i=0; i<numElements_; i++) {
            offset += sizeof(uint32_t);
            bufSize = (uint32_t*)(data_ + offset);
            offset += sizeof(uint32_t) + *bufSize;
            if (*bufSize == 0) {
                fprintf(stderr, "%lu, bufsize: %lu, %ld\n", offset, *bufSize, *(uint32_t*)(data_ + offset + 16));
                assert(false);
            }
        }
#endif
        return true;
    }
}

