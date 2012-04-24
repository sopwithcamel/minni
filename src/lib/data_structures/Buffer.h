#ifndef LIB_COMPRESS_BUFFER_H
#define LIB_COMPRESS_BUFFER_H
#include <stdint.h>
#include <vector>

namespace compresstree {
    class Buffer {
        public:
          class List {
            public:
              enum ListState {
                  DECOMPRESSED,
                  COMPRESSED,
                  PAGED_OUT
              };
              List();
              ~List();
              /* allocates buffers */
              void allocate(bool isLarge);
              /* frees allocated buffers. Maintains counter info */
              void deallocate();
              /* set list to empty */
              void setEmpty();

              uint32_t* hashes_;
              uint32_t* sizes_;
              char* data_;
              uint32_t num_;
              uint32_t size_;
              ListState state_;
              size_t c_hashlen_;
              size_t c_sizelen_;
              size_t c_datalen_;
          };
          Buffer();
          // clears all buffer state
          ~Buffer();
          // add a list and allocate memory
          List* addList(bool isLarge=false);
          void addList(List* l);
          /* clear the lists_ vector. This does not free space allocated
           * for the buffers but merely deletes the pointers. To avoid
           * memory leaks, this must be called after deallocate() */
          void delList(uint32_t ind);
          void clear();
          /* frees buffers in all the lists. This maintains all the count
           * information about each of the lists */
          void deallocate();
          /* returns true if the sum of all the list_s[i]->num_ is zero. This
           * can happen even if no memory is allocated to the buffers as all
           * buffers may be compressed */
          bool empty() const;
          uint32_t numElements() const;
          std::vector<List*> lists_;
    };
}

#endif
