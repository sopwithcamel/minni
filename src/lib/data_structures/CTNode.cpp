#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "CompressTree.h"
#include "CTNode.h"
#include "Slaves.h"

namespace compresstree {
    Node::Node(CompressTree* tree, uint32_t level) :
        tree_(tree),
        level_(level),
        parent_(NULL),
        queuedForEmptying_(false)
    {
        id_ = tree_->nodeCtr++;
        buffer_.setParent(this); 
        buffer_.setupPaging(); 

        pthread_mutex_init(&queuedForEmptyMutex_, NULL);
        tree_->createPAO_(NULL, (PartialAgg**)&lastPAO);
        tree_->createPAO_(NULL, (PartialAgg**)&thisPAO);
    }

    Node::~Node()
    {
        pthread_mutex_destroy(&queuedForEmptyMutex_);

        tree_->destroyPAO_(lastPAO);
        tree_->destroyPAO_(thisPAO);

        buffer_.cleanupPaging();
    }

    bool Node::insert(uint64_t hash, PartialAgg* agg)
    {
        uint32_t hashv = (uint32_t)hash;
        uint32_t buf_size = ((ProtobufPartialAgg*)agg)->serializedSize();

        // copy into Buffer fields
        Buffer::List* l = buffer_.lists_[0];
        l->hashes_[l->num_] = hashv;
        l->sizes_[l->num_] = buf_size;
        memset(l->data_ + l->size_, 0, buf_size);
        ((ProtobufPartialAgg*)agg)->serialize(l->data_ + l->size_,
                buf_size);
        l->size_ += buf_size;
        l->num_++;
#ifdef ENABLE_COUNTERS
        tree_->monitor_->numElements++;
#endif

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
        if (tree_->emptyType_ == ALWAYS || isFull()) {
            scheduleBufferCompressAction(Buffer::DECOMPRESS);
            tree_->sorter_->addNode(this);
            tree_->sorter_->wakeup();
        } else {
            scheduleBufferCompressAction(Buffer::COMPRESS);
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
                scheduleBufferCompressAction(Buffer::COMPRESS);
/* Already being called from above
#ifdef ENABLE_PAGING
                scheduleBufferPageAction(Buffer::PAGE_OUT);
#endif
*/
            }
            return true;
        }

        if (buffer_.empty()) {
            for (curChild=0; curChild < children_.size(); curChild++) {
                children_[curChild]->emptyOrCompress();
            }
        } else {
            checkSerializationIntegrity();
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
#ifdef ENABLE_ASSERT_CHECKS
            // there has to be a single list in the buffer at this point
            assert(buffer_.lists_.size() == 1);
#endif
            while (curElement < num) {
                if (l->hashes_[curElement] >= 
                        children_[curChild]->separator_) {
                    /* this separator is the largest separator that is not greater
                     * than *curHash. This invariant needs to be maintained.
                     */
                    if (curElement > lastElement) {
                        // copy elements into child
                        children_[curChild]->copyIntoBuffer(l, lastElement,
                                curElement - lastElement);
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "Copied %u elements into node %d\
                                 list:%u\n",
                                curElement - lastElement,
                                children_[curChild]->id_,
                                children_[curChild]->buffer_.lists_.size()-1);
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
                children_[curChild]->copyIntoBuffer(l, lastElement,
                        curElement - lastElement);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Copied %u elements into node %d; \
                        list: %u\n",
                        curElement - lastElement,
                        children_[curChild]->id_,
                        children_[curChild]->buffer_.lists_.size()-1);
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

        // initialize auxiliary buffer
        Buffer aux;
        Buffer::List* a = aux.addList();

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
                            l->sizes_[lastIndex])) {
                        fprintf(stderr, "Error at index %d\n", i);
                        assert(false);
                    }
                }
                assert(thisPAO->deserialize(perm_[i], l->sizes_[i]));
                if (!thisPAO->key().compare(lastPAO->key())) {
                    lastPAO->merge(thisPAO);
#ifdef ENABLE_COUNTERS
                    tree_->monitor_->numElements--;
                    tree_->monitor_->numMerged++;
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
                memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                memmove(a->data_ + a->size_,
                        (void*)(perm_[lastIndex]),
                        l->sizes_[lastIndex]);
                a->size_ += l->sizes_[lastIndex];
            } else {
                uint32_t buf_size = lastPAO->serializedSize();
                lastPAO->serialize(a->data_ + a->size_, buf_size);

                a->sizes_[a->num_] = buf_size;
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
            memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
            memmove(a->data_ + a->size_,
                    (void*)(perm_[lastIndex]),
                    l->sizes_[lastIndex]);
            a->size_ += l->sizes_[lastIndex];
        } else {
            uint32_t buf_size = lastPAO->serializedSize();
            lastPAO->serialize(a->data_ + a->size_, buf_size);

            a->hashes_[a->num_] = l->hashes_[lastIndex];
            a->sizes_[a->num_] = buf_size;
            a->size_ += buf_size;
        }
        a->num_++;

        // free pointer memory
        free(perm_);

        // clear buffer and shallow copy aux into buffer
        // aux is on stack and will be destroyed

        buffer_.deallocate();
        buffer_.lists_ = aux.lists_;
        aux.clear();
        checkSerializationIntegrity();
        return true;
    }

    bool Node::mergeBuffer()
    {
        std::priority_queue<Node::MergeElement, 
                std::vector<Node::MergeElement>,
                MergeComparator> queue;

        if (buffer_.lists_.size() == 1 || buffer_.empty())
            return true;

        checkSerializationIntegrity();
        // initialize aux buffer
        Buffer aux;
        Buffer::List* a;
        if (buffer_.numElements() < MAX_ELS_PER_BUFFER)
            a = aux.addList();
        else
            a = aux.addList(/*large buffer=*/true);

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
            memset(a->data_ + a->size_, 0, buf_size);
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
        buffer_.lists_ = aux.lists_;
        aux.clear();
        checkSerializationIntegrity();

        return true;
    }

    bool Node::aggregateMergedBuffer()
    {
        if (buffer_.empty())
            return true;
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
/*
                        // skip error
                        lastIndex = i;
                        lastOffset = offset;
                        offset += l->sizes_[i];
                        continue;
*/
                        fprintf(stderr, "Can't deserialize at %u, index: %u\n", lastOffset, lastIndex);
                        assert(false);
                    }
                }
                if(!thisPAO->deserialize(l->data_ + offset, l->sizes_[i])) {
/*
                    offset += l->sizes_[i];
                    continue;
*/
                    fprintf(stderr, "Can't deserialize at %u, index: %u\n", offset, i);
                    assert(false);
                }
                if (!thisPAO->key().compare(lastPAO->key())) {
                    lastPAO->merge(thisPAO);
#ifdef ENABLE_COUNTERS
                    tree_->monitor_->numElements--;
                    tree_->monitor_->numMerged++;
#endif
                    numMerged++;
                    offset += l->sizes_[i];
                    continue;
                }
            }
            a->hashes_[a->num_] = l->hashes_[lastIndex];
            if (numMerged == 0) {
                uint32_t buf_size = l->sizes_[lastIndex];
                a->sizes_[a->num_] = l->sizes_[lastIndex];
                memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                memmove(a->data_ + a->size_,
                        (void*)(l->data_ + lastOffset), l->sizes_[lastIndex]);
                a->size_ += buf_size;
            } else {
                uint32_t buf_size = lastPAO->serializedSize();
                lastPAO->serialize(a->data_ + a->size_, buf_size);
                a->sizes_[a->num_] = buf_size;
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
            memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
            memmove(a->data_ + a->size_,
                    (void*)(l->data_ + lastOffset), l->sizes_[lastIndex]);
            a->size_ += buf_size;
        } else {
            uint32_t buf_size = lastPAO->serializedSize();
            lastPAO->serialize(a->data_ + a->size_, buf_size);
            a->sizes_[a->num_] = buf_size;
            a->size_ += buf_size;
        }
        a->num_++;

        // clear buffer and copy over aux.
        // aux itself is on the stack and will be destroyed
        buffer_.deallocate();
        buffer_.clear();
        buffer_.lists_ = aux.lists_;
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

        checkSerializationIntegrity();
        // create new leaf
        Node* newLeaf = new Node(tree_, 0);
        newLeaf->copyIntoBuffer(l, splitIndex, num - splitIndex);
        newLeaf->separator_ = separator_;

        // modify this leaf properties

        // copy the first half into another list in this buffer and delete
        // the original list
        copyIntoBuffer(l, 0, splitIndex);
        separator_ = l->hashes_[splitIndex];
        // delete the old list
        buffer_.delList(0);
        l = buffer_.lists_[0];

        // check integrity of both leaves
        newLeaf->checkIntegrity();
        checkIntegrity();
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new indices: %u and\
                %u; new separators: %u and %u\n", id_, newLeaf->id_, 
                l->num_, newLeaf->buffer_.lists_[0]->num_, separator_, 
                newLeaf->separator_);
#endif

        // if leaf is also the root, create new root
        if (isRoot()) {
            buffer_.setCompressible(true);
#ifdef ENABLE_PAGING
            buffer_.setPageable(true);
#endif
            tree_->createNewRoot(newLeaf);
        } else {
            parent_->addChild(newLeaf);
        }
        return newLeaf;
    }

    bool Node::copyIntoBuffer(Buffer::List* parent_list, uint32_t index, 
            uint32_t num)
    {
        // check if the node is still queued up for a previous compression
        waitForCompressAction(Buffer::COMPRESS);

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
        assert(parent_list->state_ == Buffer::List::DECOMPRESSED);
        if (num_bytes >= BUFFER_SIZE) {
            fprintf(stderr, "Node: %d, buf: %d\n", id_, 
                    num_bytes); 
            assert(false);
        }
#endif
        // allocate a new List in the buffer and copy data into it
        Buffer::List* l = buffer_.addList();
        memset(l->hashes_, 0, num * sizeof(uint32_t));
        memmove(l->hashes_, parent_list->hashes_ + index,
                num * sizeof(uint32_t));
        memset(l->sizes_, 0, num * sizeof(uint32_t));
        memmove(l->sizes_, parent_list->sizes_ + index,
                num * sizeof(uint32_t));
        memset(l->data_, 0, num_bytes);
        memmove(l->data_, parent_list->data_ + offset, 
                num_bytes);
        l->num_ = num;
        l->size_ = num_bytes;
        checkSerializationIntegrity(buffer_.lists_.size()-1);
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
            buffer_.setCompressible(true);
#ifdef ENABLE_PAGING
            buffer_.setPageable(true);
#endif
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

    void Node::scheduleBufferCompressAction(const Buffer::CompressionAction& act)
    {
        if (!buffer_.compressible_) {
            fprintf(stderr, "Node %d not compressible\n", id_);
            return;
        }
        bool add;
        if (act == Buffer::COMPRESS)
            add = buffer_.checkCompress();
        else if (act == Buffer::DECOMPRESS)
            add = buffer_.checkDecompress();
        else
            assert(false && "Invalid compress action");
        if (add) {
            tree_->compressor_->addNode(this);
            tree_->compressor_->wakeup();
        }
    }
    
    void Node::waitForCompressAction(const Buffer::CompressionAction& act)
    {
        buffer_.waitForCompressAction(act);
    }

    void Node::performCompressAction()
    {
        buffer_.performCompressAction();
    }

    Buffer::CompressionAction Node::getCompressAction()
    {
        return buffer_.getCompressAction();
    }

#ifdef ENABLE_PAGING
    void Node::scheduleBufferPageAction(const Buffer::PageAction& act)
    {
        if (!buffer_.pageable_) {
            fprintf(stderr, "Node %d not pageable\n", id_);
            return;
        }
        bool add;
        if (act == Buffer::PAGE_OUT)
            add = buffer_.checkPageOut();
        else if (act == Buffer::PAGE_IN)
            add = buffer_.checkPageIn();
        else
            assert(false && "Invalid page action");
        if (add) {
            tree_->pager_->addNode(this);
            tree_->pager_->wakeup();
        }
    }

    void Node::waitForPageAction(const Buffer::PageAction& act)
    {
        buffer_.waitForPageAction(act);
    }

    void Node::performPageAction()
    {
        buffer_.performPageAction();
#ifdef CT_NODE_DEBUG
        Buffer::PageAction act = getPageAction();
        if (act == Buffer::PAGE_OUT)
            fprintf(stderr, "pager: paged out node: %d\n", id_);
        else if (act == Buffer::PAGE_IN)
            fprintf(stderr, "pager: paged in node: %d\n", id_);
#endif
    }

    Buffer::PageAction Node::getPageAction()
    {
        return buffer_.getPageAction();
    }
#endif // ENABLE_PAGING

    bool Node::checkSerializationIntegrity(int listn/*=-1*/)
    {
#if 0
        uint32_t offset;
        PartialAgg* pao;
        tree_->createPAO_(NULL, &pao);
        if (listn < 0) {
            for (uint32_t j=0; j<buffer_.lists_.size(); j++) {
                Buffer::List* l = buffer_.lists_[j];
                offset = 0;
                for (uint32_t i=0; i<l->num_; i++) {
                    if (!((ProtobufPartialAgg*)pao)->deserialize(l->data_ + offset, l->sizes_[i])) {
                        fprintf(stderr, "Error in list %u, index %u, offset %u\n",
                                j, i, offset);
                        assert(false);
                    }
                    offset += l->sizes_[i];
                }
            }
        } else {
            Buffer::List* l = buffer_.lists_[listn];
            offset = 0;
            for (uint32_t i=0; i<l->num_; i++) {
                if (!((ProtobufPartialAgg*)pao)->deserialize(l->data_ + offset, l->sizes_[i])) {
                    fprintf(stderr, "Error in list %u, index %u, offset %u\n",
                            listn, i, offset);
                    assert(false);
                }
                offset += l->sizes_[i];
            }
        }
        tree_->destroyPAO_(pao);
#endif
    }

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

