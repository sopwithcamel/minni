#include "Block.h"
#include "BufferTree.h"
#include "Node.h"

namespace buffertree {

    Node::Node() :
        numBlocks_(0),
        isInMemory(false)
    {
        // Allocate file name and create file
    }

    Node::~Node()
    {
    }

    bool Node::addBlock(const Block& block)
    {
        // check if buffer is loaded in memory
        if (!isInMemory()) {
            assert(loadBuffer());
        }
        // copy the entire Block into the buffer
        memmove(data_ + numBlocks_ * BLOCK_SIZE, block.data_, BLOCK_SIZE);
        numBlocks_++;
        
        // if buffer threshold is reached, call emptyBuffer
        if (numBlocks_ > BLOCKS_THRESHOLD) {
            emptyBuffer();
        }
    }

    bool Node::emptyAllBuffers()
    {
    }

    bool Node::isInMemory()
    {
        return isInMemory;
    }

    bool Node::loadBuffer()
    {
        // allocate memory for buffer
        data_ = (char*)malloc(BLOCK_SIZE * BLOCKS_PER_BUFFER);
        assert(data_ != NULL);

        // copy from buffer file
        int bfd = open(bufferFile_.c_str(), O_RDWR);
        assert(bfd > 0);
        size_t byt = read(fd, data_, BLOCK_SIZE * BLOCKS_PER_BUFFER);
        assert(byt == BLOCK_SIZE * BLOCKS_PER_BUFFER);

        close(bfd);
    }

    bool Node::emptyBuffer()
    {
    }
}

