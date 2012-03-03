#ifndef LIB_COMPRESS_BUFFER_H
#define LIB_COMPRESS_BUFFER_H
#include <stdint.h>
#include <vector>

namespace compresstree {
    class Buffer {
        public:
          class List {
            public:
              List();
              ~List();

              uint32_t* hashes_;
              uint32_t* sizes_;
              char* data_;
              uint32_t num_;
              uint32_t size_;
              size_t c_hashlen_;
              size_t c_sizelen_;
              size_t c_datalen_;
          };
          Buffer();
          // clears all buffer state
          ~Buffer();
          // add a list and allocate memory
          List* addList();
          void addList(List* l);
          // free all lists
          void clear();
          // deallocate buffers
          void deallocate();
          bool empty() const;
          std::vector<List*> lists_;
    };
}

#endif
