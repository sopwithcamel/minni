#include <stdlib.h>
#include "PartialAgg.h"
#include <assert.h>
#include <jpeglib.h>
#include <jerror.h>

#define cimg_plugin "plugins/jpeg_buffer.h"
#include "CImg.h"
using namespace cimg_library;

class ImagePAO : public PartialAgg {
  public:
	ImagePAO(const char** tokens);
	~ImagePAO();
	/* add new neighbor if it is k-nearest */ 
	void add(void* neighbor_key);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
	bool tokenize(void*, void*, void*, char**);
  private:
	uint64_t hash;
	CImg<float>* ph_dct_matrix(const int N);
	void pHash(CImg<unsigned char> img, unsigned long& hash);
};
