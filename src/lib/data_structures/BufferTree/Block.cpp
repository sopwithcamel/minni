#include "Block.h"

namespace buffertree {
    Block::Block() :
        blockOffset_(0)
    {
        // allocate memory for block
        data_ = (char*)malloc(BLOCK_SIZE);
    }

    Block::~Block()
    {
        free(data_);
    }

    bool Block::addRecord(uint64_t hash, void* buf, size_t buf_size)
    {
        // check if we have reached the end of the block
        if (blockOffset_ + sizeof(struct RecordHeader) + sizeof(uint64_t) +
                buf_size > BLOCK_SIZE) {
            assert(root_node_->addBlock(*this));
            blockOffset_ = 0;
        }

        rh.recordLength = buf_size;
        // copy record header into block
        memmove((void*)(data_ + blockOffset_, (void*)&rh, 
                sizeof(struct RecordHeader)));
        blockOffset_ += sizeof(struct RecordHeader);

        // copy hash value into block
        memmove((void*)(data_ + blockOffset_, (void*)&hash, 
                sizeof(uint64_t)));
        blockOffset_ += sizeof(uint64_t);

        // copy buffer into block
        memmove((void*)(data_ + blockOffset_), buf, buf_size);
        blockOffset_ += buf_size;
        return true;
    }

}
