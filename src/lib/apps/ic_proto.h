#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <stdlib.h>
#include "ProtobufPartialAgg.h"
#include "Tokenizer.h"
#include <assert.h>
#include <list>
#include "Neighbor.h"
#include <math.h>
#include <algorithm>
#include "imgclass.pb.h"

class ImageClassPAO : public ProtobufPartialAgg {
  public:
	ImageClassPAO(Token* token);
	~ImageClassPAO();
	static size_t create(Token* t, PartialAgg** p);
	/* merge two lists */
	void merge(PartialAgg* add_agg);
    inline const std::string& key() const;
	inline void serialize(
            google::protobuf::io::CodedOutputStream* output) const;
	inline void serialize(std::string* output) const;
	inline bool deserialize(
            google::protobuf::io::CodedInputStream* input);
	inline bool deserialize(const std::string& input);
	inline bool deserialize(const char* input, size_t size);
    inline uint32_t serializedSize() const;
    inline void serialize(char* output, size_t size);
  private:
    imgclasspao::pao pb;
};
 
inline const std::string& ImageClassPAO::key() const
{
    return pb.key();
};
