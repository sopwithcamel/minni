#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "compsort.h"
#include "CompressTree.h"
#include "CTNode.h"
#include "rle.h"
#include "Slaves.h"
#include "snappy.h"
#include "zlib.h"

namespace compresstree {
    Node::Node(CompressTree* tree, uint32_t level) :
        tree_(tree),
        level_(level),
        parent_(NULL),
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
        tree_->createPAO_(NULL, (PartialAgg**)&lastPAO);
        tree_->createPAO_(NULL, (PartialAgg**)&thisPAO);

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
        uint32_t hashv = (uint32_t)hash;
        uint32_t buf_size = value.size();

        // copy into Buffer fields
        Buffer::List* l = buffer_.lists_[0];
        l->hashes_[l->num_] = hashv;
        l->sizes_[l->num_] = buf_size;
        memmove(l->data_ + l->size_, (void*)value.data(), buf_size);
        l->size_ += buf_size;
        l->num_++;

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

#ifndef ASYNC_SORTING
    void Node::syncEmpty()
    {
        aggregateSortedBuffer();
        if (!isFull() && !isRoot() && 
                tree_->emptyType_ != ALWAYS) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "node: %d reduced in size to %u\n", 
                    id_, buffer_.numElements());
#endif
            // Set node as NOT queued for emptying
#ifdef ASYNC_COMPRESSION
            CALL_MEM_FUNC(*this, compress)();
#else
            snappyCompress();
#endif
        } else {
            emptyBuffer();
            if (isLeaf())
                tree_->handleFullLeaves();
        }
    }
#endif

    bool Node::emptyOrCompress()
    {
        if (tree_->emptyType_ == ALWAYS || isFull()) {
#ifndef STRUCTURE_BUFFER
#ifdef ASYNC_COMPRESSION
            CALL_MEM_FUNC(*this, decompress)();
#else
            if (buffer_.lists_[0]->state_ == Buffer::List::COMPRESSED)
                snappyDecompress();
#endif
#endif

#ifdef ASYNC_SORTING
            tree_->sorter_->addNode(this);
            tree_->sorter_->wakeup();
#else
            sortBuffer();
            syncEmpty();
#endif
        } else {
#ifdef ASYNC_COMPRESSION
            CALL_MEM_FUNC(*this, compress)();
#else
            snappyCompress();
#endif
        }
    }

    bool Node::emptyBuffer()
    {
        uint32_t curChild = 0;
        // offset till which elements have been written
        uint32_t offset = 0;
        uint32_t curElement = 0;
        uint32_t lastElement = 0; 

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf()) {
            /* this may be called even when buffer is not full (when flushing
             * all buffers at the end). */
            if (isFull() || isRoot()) {
                tree_->addLeafToEmpty(this);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\
                        %u/%u\n", id_, buffer_.numElements(), EMPTY_THRESHOLD);
#endif
            } else { // compress
#ifdef ASYNC_COMPRESSION
                CALL_MEM_FUNC(*this, compress)();
#else
                snappyCompress();
#endif
            }
            return true;
        }

        if (buffer_.empty()) {
            for (curChild=0; curChild < children_.size(); curChild++) {
                children_[curChild]->emptyOrCompress();
            }
        } else {
            Buffer::List* l = buffer_.lists_[0];
            // find the first separator strictly greater than the first element
            while (l->hashes_[curElement] >= 
                    children_[curChild]->separator_) {
                children_[curChild]->emptyOrCompress();
                curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                if (curChild >= children_.size()) {
                    fprintf(stderr, "Node: %d: Can't place %u among children\n", id_, 
                            l->hashes_[curElement]);
                    checkIntegrity();
                    assert(false);
                }
#endif
            }
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node: %d: first node chosen: %d (sep: %u,\
                child: %d); first element: %u\n", id_, children_[curChild]->id_,
                    children_[curChild]->separator_, curChild, l->hashes_[0]);
#endif
            uint32_t num = buffer_.numElements();
            while (curElement < num) {
                if (l->hashes_[curElement] >= 
                        children_[curChild]->separator_) {
                    /* this separator is the largest separator that is not greater
                     * than *curHash. This invariant needs to be maintained.
                     */
                    if (curElement > lastElement) {
                        // copy elements into child
#ifndef STRUCTURE_BUFFER
#ifdef ASYNC_COMPRESSION
                        children_[curChild]->waitForCompressAction(DECOMPRESS);
#else
                        children_[curChild]->snappyDecompress();
#endif
#endif
                        children_[curChild]->copyIntoBuffer(l, lastElement,
                                curElement - lastElement);
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "Copied %u elements into node %d;\
                                sep: %u, next: %u\n",
                                curElement - lastElement,
                                children_[curChild]->id_,
                                children_[curChild]->separator_,
                                buffer_.lists_[0]->hashes_[curElement]);
#endif
                        lastElement = curElement;
                    }
                    // skip past all separators not greater than the current hash
                    while (l->hashes_[curElement] 
                            >= children_[curChild]->separator_) {
                        children_[curChild]->emptyOrCompress();
                        curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                        if (curChild >= children_.size()) {
                            fprintf(stderr, "Can't place %u among children\n", 
                                    l->hashes_[curElement]);
                            assert(false);
                        }
#endif
                    }
                }
                // proceed to next element
                assert(l->sizes_[curElement] != 0);
                curElement++;
            }

            // copy remaining elements into child
            if (curElement >= lastElement) {
                // copy elements into child
#ifndef STRUCTURE_BUFFER
#ifdef ASYNC_COMPRESSION
                children_[curChild]->waitForCompressAction(DECOMPRESS);
#else
                        children_[curChild]->snappyDecompress();
#endif
#endif
                children_[curChild]->copyIntoBuffer(l, lastElement,
                        curElement - lastElement);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Copied %u elements into node %d; \
                        sep: %u\n",
                        curElement - lastElement,
                        children_[curChild]->id_,
                        children_[curChild]->separator_);
#endif
                children_[curChild]->emptyOrCompress();
                curChild++;
            }
            // empty or compress any remaining children
            while (curChild < children_.size()) {
                children_[curChild]->emptyOrCompress();
                curChild++;
            }

            // reset
            l->setEmpty();

            if (!isRoot()) {
                buffer_.deallocate();
                buffer_.clear();
            }
        }
        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
        return true;
    }

    void Node::quicksort(uint32_t uleft, uint32_t uright)
    {
        int32_t i, j, stack_pointer = -1;
        int32_t left = uleft;
        int32_t right = uright;
        int32_t* rstack = new int32_t[128];
        uint32_t swap, temp;
        uint32_t sizs, sizt;
        char *pers, *pert;
        uint32_t* arr = buffer_.lists_[0]->hashes_;
        uint32_t* siz = buffer_.lists_[0]->sizes_;
        while (true) {
            if (right - left <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = arr[j];
                    sizs = siz[j];
                    pers = perm_[j];
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
                    siz[i + 1] = sizs;
                    perm_[i + 1] = pers;
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
                sizs = siz[median]; siz[median] = siz[i]; siz[i] = sizs;
                pers = perm_[median]; perm_[median] = perm_[i]; perm_[i] = pers;
                if (arr[left] > arr[right]) {
                    swap = arr[left]; arr[left] = arr[right]; arr[right] = swap;
                    sizs = siz[left]; siz[left] = siz[right]; siz[right] = sizs;
                    pers = perm_[left]; perm_[left] = perm_[right]; perm_[right] = pers;
                }
                if (arr[i] > arr[right]) {
                    swap = arr[i]; arr[i] = arr[right]; arr[right] = swap;
                    sizs = siz[i]; siz[i] = siz[right]; siz[right] = sizs;
                    pers = perm_[i]; perm_[i] = perm_[right]; perm_[right] = pers;
                }
                if (arr[left] > arr[i]) {
                    swap = arr[left]; arr[left] = arr[i]; arr[i] = swap;
                    sizs = siz[left]; siz[left] = siz[i]; siz[i] = sizs;
                    pers = perm_[left]; perm_[left] = perm_[i]; perm_[i] = pers;
                }
                temp = arr[i];
                sizt = siz[i];
                pert = perm_[i];
                while (true) {
                    while (arr[++i] < temp);
                    while (arr[--j] > temp);
                    if (j < i) {
                        break;
                    }
                    swap = arr[i]; arr[i] = arr[j]; arr[j] = swap;
                    sizs = siz[i]; siz[i] = siz[j]; siz[j] = sizs;
                    pers = perm_[i]; perm_[i] = perm_[j]; perm_[j] = pers;
                }
                arr[left + 1] = arr[j];
                siz[left + 1] = siz[j];
                perm_[left + 1] = perm_[j];
                arr[j] = temp;
                siz[j] = sizt;
                perm_[j] = pert;
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
        if (buffer_.empty())
            return true;
        // initialize pointers to serialized PAOs
        uint32_t num = buffer_.numElements();
        perm_ = (char**)malloc(sizeof(char*) * num);
        uint32_t offset = 0;
        for (uint32_t i=0; i<num; i++) {
            perm_[i] = buffer_.lists_[0]->data_ + offset;
            offset += buffer_.lists_[0]->sizes_[i];
        }

        // quicksort elements
        quicksort(0, num - 1);
        checkIntegrity();
    }

    bool Node::aggregateSortedBuffer()
    {
        uint32_t buf_size;

        if (buffer_.empty())
            return true;

        // initialize auxiliary buffer
        Buffer aux;
        Buffer::List* a;
        if (buffer_.numElements() < MAX_ELS_PER_BUFFER)
            a = aux.addList();
        else
            a = aux.addList(/*isLarge=*/true);

        // exit condition
        bool last_serialized = false;

        // aggregate elements in buffer
        uint32_t lastIndex = 0;
        Buffer::List* l = buffer_.lists_[0];
        for (uint32_t i=1; i<l->num_; i++) {
            if (l->hashes_[i] == l->hashes_[lastIndex]) {
                // aggregate elements
                if (i == lastIndex + 1) {
                    if (!lastPAO->deserialize(perm_[lastIndex], 
                            l->sizes_[i])) {
                        fprintf(stderr, "Error at index %u\n", lastIndex);
                        assert(false);
                    }
                }
                assert(thisPAO->deserialize(perm_[i], l->sizes_[i]));
                if (!thisPAO->key().compare(lastPAO->key())) {
                    lastPAO->merge(thisPAO);
#ifdef ENABLE_COUNTERS
                    tree_->monitor_->cctr++;
#endif
                    continue;
                }
            }
            // copy hash and size into auxBuffer_
            a->hashes_[a->num_] = l->hashes_[lastIndex];
            if (i == lastIndex + 1) {
                // the size wouldn't have changed
                a->sizes_[a->num_] = l->sizes_[lastIndex];
                memmove(a->data_ + a->size_,
                        (void*)(perm_[lastIndex]),
                        l->sizes_[lastIndex]);
                a->size_ += l->sizes_[lastIndex];
            } else {
                std::string serialized;
                lastPAO->serialize(&serialized);
                uint32_t buf_size = serialized.size();

                a->sizes_[a->num_] = buf_size;
                memmove(a->data_ + a->size_,
                        (void*)serialized.data(), buf_size);                        
                a->size_ += buf_size;
#ifdef ENABLE_COUNTERS
                tree_->monitor_->bctr++;
#endif
            }
            a->num_++;
            lastIndex = i;
        }
        // copy the last PAO; TODO: Clean this up!
        // copy hash and size into auxBuffer_
        if (lastIndex == l->num_-1) {
            a->hashes_[a->num_] = l->hashes_[lastIndex];
            // the size wouldn't have changed
            a->sizes_[a->num_] = l->sizes_[lastIndex];
            memmove(a->data_ + a->size_,
                    (void*)(perm_[lastIndex]),
                    l->sizes_[lastIndex]);
            a->size_ += l->sizes_[lastIndex];
        } else {
            std::string serialized;
            lastPAO->serialize(&serialized);
            uint32_t buf_size = serialized.size();

            a->hashes_[a->num_] = l->hashes_[lastIndex];
            a->sizes_[a->num_] = buf_size;
            memmove(a->data_ + a->size_,
                    (void*)serialized.data(), buf_size);                        
            a->size_ += buf_size;
        }
        a->num_++;

        // free pointer memory
        free(perm_);

        // clear buffer and shallow copy aux into buffer
        // aux is on stack and will be destroyed

        buffer_.deallocate();
        buffer_ = aux;
        aux.clear();
        return true;
    }

    bool Node::mergeBuffer()
    {
        std::priority_queue<Node::MergeElement, 
                std::vector<Node::MergeElement>,
                MergeComparator> queue;

        if (buffer_.lists_.size() == 1 || buffer_.empty())
            return true;

        // initialize aux buffer
        Buffer aux;
        Buffer::List* a;
        if (buffer_.numElements() < MAX_ELS_PER_BUFFER)
            a = aux.addList();
        else
            a = aux.addList(/*isLarge=*/true);

        // track number of PAOs that have been merged with lastPAO
        uint32_t numMerged = 0;

        // Load each of the list heads into the priority queue
        // keep track of offsets for possible deserialization
        for (int i=0; i<buffer_.lists_.size(); i++) {
            if (buffer_.lists_[i]->num_ > 0) {
                Node::MergeElement* mge = new Node::MergeElement(
                        buffer_.lists_[i]);
                queue.push(*mge);
            }
        }

        while (!queue.empty()) {
            Node::MergeElement n = queue.top();
            queue.pop();

            // copy hash values
            a->hashes_[a->num_] = n.hash();
            uint32_t buf_size = n.size();
            a->sizes_[a->num_] = buf_size;
            memmove(a->data_ + a->size_,
                    (void*)n.data(), buf_size);
            a->size_ += buf_size;
            a->num_++;
/*
            if (a->num_ >= MAX_ELS_PER_BUFFER) {
                fprintf(stderr, "Num elements: %u\n", a->num_);
                assert(false);
            }
*/
            // increment n pointer and re-insert n into prioQ
            if (n.next())
                queue.push(n);
        }

        // clear buffer and copy over aux.
        // aux itself is on the stack and will be destroyed
        buffer_.deallocate();
        buffer_.clear();
        buffer_ = aux;
        aux.clear();

        return true;
    }

    bool Node::aggregateMergedBuffer()
    {
        if (buffer_.empty())
            return true;
        assert(buffer_.lists_.size() == 1);
        // initialize aux buffer
        Buffer aux;
        Buffer::List* a;
        if (buffer_.numElements() < MAX_ELS_PER_BUFFER)
            a = aux.addList();
        else
            a = aux.addList(/*isLarge=*/true);

        Buffer::List* l = buffer_.lists_[0];
        uint32_t num = buffer_.numElements();
        uint32_t numMerged = 0;
        uint32_t offset = 0;

        uint32_t lastIndex = 0;
        offset += l->sizes_[0];
        uint32_t lastOffset = 0;

        // aggregate elements in buffer
        for (uint32_t i=1; i<num; i++) {
            if (l->hashes_[i] == l->hashes_[lastIndex]) {
                if (numMerged == 0) {
                    if (!lastPAO->deserialize(l->data_ + lastOffset,
                            l->sizes_[lastIndex])) {
                        fprintf(stderr, "Error at %u\n", lastIndex);
                        assert(false);
                    }
                }
                if(!thisPAO->deserialize(l->data_ + offset, l->sizes_[i])) {
                    assert(false);
                }
                if (!thisPAO->key().compare(lastPAO->key())) {
                    lastPAO->merge(thisPAO);
                    numMerged++;
                    offset += l->sizes_[i];
                    continue;
                }
            }
            a->hashes_[a->num_] = l->hashes_[lastIndex];
            if (numMerged == 0) {
                uint32_t buf_size = l->sizes_[lastIndex];
                a->sizes_[a->num_] = l->sizes_[lastIndex];
                memmove(a->data_ + a->size_,
                        (void*)(l->data_ + lastOffset), l->sizes_[lastIndex]);
                a->size_ += buf_size;
            } else {
                std::string serialized;
                lastPAO->serialize(&serialized);
                uint32_t buf_size = serialized.size();
                a->sizes_[a->num_] = buf_size;
                memmove(a->data_ + a->size_,
                        (void*)(serialized.data()), buf_size);
                a->size_ += buf_size;
            }
            a->num_++;
            numMerged = 0;
            lastIndex = i;
            lastOffset = offset;
            offset += l->sizes_[i];
        }
        // copy last PAO
        a->hashes_[a->num_] = l->hashes_[lastIndex];
        if (numMerged == 0) {
            uint32_t buf_size = l->sizes_[lastIndex];
            a->sizes_[a->num_] = l->sizes_[lastIndex];
            memmove(a->data_ + a->size_,
                    (void*)(l->data_ + lastOffset), l->sizes_[lastIndex]);
            a->size_ += buf_size;
        } else {
            std::string serialized;
            lastPAO->serialize(&serialized);
            uint32_t buf_size = serialized.size();
            a->sizes_[a->num_] = buf_size;
            memmove(a->data_ + a->size_,
                    (void*)(serialized.data()), buf_size);
            a->size_ += buf_size;
        }
        a->num_++;

        // clear buffer and copy over aux.
        // aux itself is on the stack and will be destroyed
        buffer_.deallocate();
        buffer_.clear();
        buffer_ = aux;
        aux.clear();
        return true;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::splitLeaf()
    {
        checkIntegrity();

        // select splitting index
        uint32_t num = buffer_.numElements();
        uint32_t splitIndex = num/2;
        Buffer::List* l = buffer_.lists_[0];
        while (l->hashes_[splitIndex] == l->hashes_[splitIndex-1]) {
            splitIndex++;
#ifdef ENABLE_ASSERT_CHECKS
            if (splitIndex == num) {
                assert(false);
            }                
#endif
        }

        // create new leaf
        Node* newLeaf = new Node(tree_, 0);
        newLeaf->copyIntoBuffer(l, splitIndex, num - splitIndex);
        newLeaf->separator_ = separator_;

        // modify this leaf properties
#ifndef STRUCTURE_BUFFER
        uint32_t offset = 0;
        for (uint32_t i=0; i<splitIndex; i++) {
            offset += l->sizes_[i];
        }
        l->size_ = offset;
        l->num_ = splitIndex;
#else
        // copy the first half into another list in this buffer and delete
        // the original list
        copyIntoBuffer(l, 0, splitIndex);
#endif
        separator_ = l->hashes_[splitIndex];

        // check integrity of both leaves
        newLeaf->checkIntegrity();
        checkIntegrity();
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new offsets: %u and\
                %u; new separators: %u and %u\n", id_, newLeaf->id_, 
                l->size_, newLeaf->buffer_.lists_[0]->size_, separator_, 
                newLeaf->separator_);
#endif
        // delete the old list
        buffer_.delList(0);
        l = NULL;

        // if leaf is also the root, create new root
        if (isRoot()) {
            setCompressible(true);
            tree_->createNewRoot(newLeaf);
        } else {
            parent_->addChild(newLeaf);
        }
        return newLeaf;
    }

#ifdef STRUCTURE_BUFFER
    bool Node::copyIntoBuffer(Buffer::List* parent_list, uint32_t index, 
            uint32_t num)
    {
        // calculate offset
        uint32_t offset = 0;
        uint32_t num_bytes = 0;
        for (uint32_t i=0; i<index; i++) {
            offset += parent_list->sizes_[i];
        }
        for (uint32_t i=0; i<num; i++) {
            num_bytes += parent_list->sizes_[index + i];
        }
#ifdef ENABLE_ASSERT_CHECKS
        if (num_bytes >= BUFFER_SIZE) {
            fprintf(stderr, "Node: %d, buf: %d\n", id_, 
                    num_bytes); 
            assert(false);
        }
#endif
        // allocate a new List in the buffer and copy data into it
        Buffer::List* l = buffer_.addList();
        memmove(l->hashes_, parent_list->hashes_ + index,
                num * sizeof(uint32_t));
        memmove(l->sizes_, parent_list->sizes_ + index,
                num * sizeof(uint32_t));
        memmove(l->data_, parent_list->data_ + offset, 
                num_bytes);
        l->num_ = num;
        l->size_ = num_bytes;
        return true;
    }
#else
    bool Node::copyIntoBuffer(Buffer::List* parent_list, uint32_t index, 
            uint32_t num)
    {
        Buffer::List* l;
        if (buffer_.lists_.size() == 0)
            l = buffer_.addList();
        else
            l = buffer_.lists_[0];
        // calculate offset
        uint32_t offset = 0;
        uint32_t num_bytes = 0;
        for (uint32_t i=0; i<index; i++) {
            offset += parent_list->sizes_[i];
        }
        for (uint32_t i=0; i<num; i++) {
            num_bytes += parent_list->sizes_[index + i];
        }
        if (num_bytes + l->size_ >= BUFFER_SIZE || l->num_ + num >= MAX_ELS_PER_BUFFER) {
            Buffer aux;
            Buffer::List* a = aux.addList(/*isLarge=*/true);
            // move data to new list
            memmove(a->hashes_, l->hashes_, l->num_ * sizeof(uint32_t));
            memmove(a->sizes_, l->sizes_, l->num_ * sizeof(uint32_t));
            memmove(a->data_, l->data_, l->size_);
            a->num_ = l->num_;
            a->size_ = l->size_;            
            buffer_.deallocate();
            buffer_.clear();
            buffer_ = aux;
            aux.clear();
            l = buffer_.lists_[0];
        }
        memmove(l->hashes_ + l->num_, parent_list->hashes_ + index,
                num * sizeof(uint32_t));
        memmove(l->sizes_ + l->num_, parent_list->sizes_ + index,
                num * sizeof(uint32_t));
        memmove(l->data_ + l->size_, parent_list->data_ + offset, 
                num_bytes);
        l->num_ += num;
        l->size_ += num_bytes;
        return true;
    }
#endif

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
        if (!buffer_.empty()) {
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
            fprintf(stderr, "%d sep is %u and %d sep is %u\n", 
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
            fprintf(stderr, "%u, ", children_[j]->separator_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j=0; j<newNode->children_.size(); j++)
            fprintf(stderr, "%u, ", newNode->children_[j]->separator_);
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
            setCompressible(true);
            buffer_.deallocate();
            buffer_.clear();
            return tree_->createNewRoot(newNode);
        } else
            return parent_->addChild(newNode);
    }

    bool Node::isFull() const
    {
        if (buffer_.numElements() > EMPTY_THRESHOLD)
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
            if (buffer_.empty()) {
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
        if (!compressible_) {
            fprintf(stderr, "Node %d not compressible\n", id_);
            return true;
        }
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
            if (buffer_.empty()) {
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
        if (!buffer_.empty()) {
            // allocate memory for one list
            Buffer compressed;

            for (uint32_t i=0; i<buffer_.lists_.size(); i++) {
                Buffer::List* l = buffer_.lists_[i];
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

    bool Node::snappyDecompress()
    {
        if (!buffer_.empty()) {
            // allocate memory for decompressed buffers
            Buffer decompressed;

            for (uint32_t i=0; i<buffer_.lists_.size(); i++) {
                Buffer::List* cl = buffer_.lists_[i];
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
            ret = pwrite(fd_, buffer_, compLength_, 0);
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
            free(buffer_);
        }
        buffer_ = NULL;

        setState(PAGED_OUT);
        return true;
    }

    bool Node::pageIn()
    {
        if (compLength_ > 0) {
            buffer_ = (char*)malloc(BUFFER_SIZE);
            size_t ret = pread(fd_, buffer_, compLength_, 0);
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
        uint32_t offset;
        uint32_t* curHash;
        offset = 0;
        for (uint32_t j=0; j<buffer_.lists_.size(); j++) {
            Buffer::List* l = buffer_.lists_[j];
            for (uint32_t i=0; i<l->num_-1; i++) {
                if (l->hashes_[i] > l->hashes_[i+1]) {
                    fprintf(stderr, "Node: %d, List: %d: Hash %u at index %u\
                            greater than hash %u at %u (size: %u)\n", id_, j,
                            l->hashes_[i], i, l->hashes_[i+1],
                            i+1, l->num_);
                    assert(false);
                }
            }
            for (uint32_t i=0; i<l->num_; i++) {
                if (l->sizes_[i] == 0) {
                    fprintf(stderr, "Element %u in list %u has 0 size; tot size: %u\n", i, j, l->num_);
                    assert(false);
                }
            }
            if (l->hashes_[l->num_-1] >= separator_) {
                fprintf(stderr, "Node: %d: Hash %u at index %u\
                        greater than separator %u\n", id_, 
                        l->hashes_[l->num_-1], l->num_-1, separator_);
                assert(false);
            }
        }
#endif
        return true;
    }
}

