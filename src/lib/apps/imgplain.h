#include "Mapper.h"
#include <stdlib.h>
#include "BoostPartialAgg.h"
#include "Tokenizer.h"
#include <assert.h>
#include <jpeglib.h>
#include <jerror.h>
#include <list>
#include "Neighbor.h"
#include <math.h>

#define cimg_plugin "plugins/jpeg_buffer.h"
#include "CImg.h"
using namespace cimg_library;

class ImagePlain : public BoostPartialAgg {
  public:
	ImagePlain(Token* token);
	~ImagePlain();
    inline const std::string& key() const;
	static size_t create(Token* t, PartialAgg** p);
	void merge(PartialAgg* add_agg);
	inline void serialize(boost::archive::binary_oarchive* output) const;
	inline bool deserialize(boost::archive::binary_iarchive* input);

  private:
    std::string key_;
    std::list<Neighbor>* neigh_list;
	uint64_t hash;
	CImg<float>* ph_dct_matrix(const int N);
	void pHash(CImg<unsigned char> img, uint64_t& hash);
};
