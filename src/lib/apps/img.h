#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <stdlib.h>
#include "ProtobufPartialAgg.h"
#include "Tokenizer.h"
#include <assert.h>
#include <jpeglib.h>
#include <jerror.h>
#include <list>
#include "Neighbor.h"
#include <math.h>
#include <algorithm>
#include "imgpao.pb.h"

#define cimg_plugin "plugins/jpeg_buffer.h"
#include "CImg.h"
using namespace cimg_library;

class ImagePAO : public ProtobufPartialAgg {
  public:
	ImagePAO(Token* token);
	~ImagePAO();
	static size_t create(Token* t, PartialAgg** p);
	/* add new neighbor if it is k-nearest */ 
	//void add(void* neighbor_key);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
    inline const std::string& key() const;
    inline google::protobuf::RepeatedPtrField<imagepao::Neighbor>* mutable_neighbors();
    inline const google::protobuf::RepeatedPtrField<imagepao::Neighbor>& neighbors();
    inline int neighbors_size() const;
    inline const imagepao::Neighbor& neighbor(int index) const;
    inline imagepao::Neighbor* mutable_neighbor(int index);
	inline void serialize(
            google::protobuf::io::CodedOutputStream* output) const;
	inline void serialize(std::string* output) const;
	inline bool deserialize(
            google::protobuf::io::CodedInputStream* input);
	inline bool deserialize(const std::string& input);
	inline bool deserialize(const char* input, size_t size);
    inline uint32_t serializedSize() const;
    inline void serialize(char* output, size_t size);
    static bool comparator(imagepao::Neighbor n1, imagepao::Neighbor n2)
    {
        return (n1.distance() < n2.distance());
    }

  private:
    imagepao::pao pb;
	uint64_t hash;
	CImg<float>* ph_dct_matrix(const int N);
	void pHash(CImg<unsigned char> img, uint64_t& hash);
};
 
inline const google::protobuf::RepeatedPtrField<imagepao::Neighbor>& ImagePAO::neighbors()
{
    return pb.neighbors();
}

inline google::protobuf::RepeatedPtrField<imagepao::Neighbor>*  ImagePAO::mutable_neighbors()
{
    return pb.mutable_neighbors();
}

inline const std::string& ImagePAO::key() const
{
    return pb.key();
};

inline int ImagePAO::neighbors_size() const
{
    return pb.neighbors_size();
};

inline imagepao::Neighbor* ImagePAO::mutable_neighbor(int index)
{
    return pb.mutable_neighbors(index);
};

inline const imagepao::Neighbor& ImagePAO::neighbor(int index) const
{
    return pb.neighbors(index);
};
