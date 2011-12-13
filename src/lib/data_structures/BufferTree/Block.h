#ifndef LIB_BT_BLOCK_H
#define LIB_BT_BLOCK_H

namespace BufferTree {

    const size_t BLOCK_SIZE = 4096;

    struct RecordHeader {
        uint32_t recordLength;
    };

    class Block {
      public:
        Block();
        ~Block();
        /* Copy the record into the block. If the block is full, all records
         * are copied into the root of the buffer tree */
        bool addRecord(uint64_t hash, void* buf, size_t buf_size);
      private:
        char* data_;
        Node* root_node_; // root of the buffer tree for overflow
        struct RecordHeader rh;
        size_t blockOffset_; // track where to write the next record
    };
}

#endif
