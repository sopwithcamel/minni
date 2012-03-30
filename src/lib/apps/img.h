#include <stdlib.h>
#include "PartialAgg.h"
#include "Tokenizer.h"
#include <assert.h>
#include <jpeglib.h>
#include <jerror.h>
#include <list>
#include "Neighbor.h"
#include <math.h>
#include "wordcountpao.pb.h"

#define cimg_plugin "plugins/jpeg_buffer.h"
#include "CImg.h"
using namespace cimg_library;

class ImagePAO : public PartialAgg {
  public:
	ImagePAO(Token* token);
	~ImagePAO();
	static size_t create(Token* t, PartialAgg** p);
	/* add new neighbor if it is k-nearest */ 
	void add(void* neighbor_key);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
	void serialize(FILE* f, void* buf, size_t buf_size);
	void serialize(void* buf);
	bool deserialize(FILE* f, void* buf, size_t buf_size);
	bool deserialize(void* buf);
  private:
	std::list<Neighbor>* neigh_list;
	uint64_t hash;
	CImg<float>* ph_dct_matrix(const int N);
	void pHash(CImg<unsigned char> img, unsigned long& hash);
};
