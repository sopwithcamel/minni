#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "Mapper.h"
#include "ProtobufPartialAgg.h"
#include "Tokenizer.h"
#include "wcdigram.pb.h"

class WCDigram : public ProtobufPartialAgg {
  public:
	WCDigram(char* one, char* two, char* three);
	~WCDigram();
    inline const std::string& key() const;
    inline const uint64_t count() const;
	static size_t create(Token* t, PartialAgg** p);
//	void add(void* value);
	void merge(PartialAgg* add_agg);
    inline uint32_t serializedSize() const;
	inline void serialize(
            google::protobuf::io::CodedOutputStream* output) const;
	inline void serialize(std::string* output) const;
	inline void serialize(char* output, size_t size);
	inline bool deserialize(
            google::protobuf::io::CodedInputStream* input);
	inline bool deserialize(const std::string& input);
	inline bool deserialize(const char* input, size_t size);
  private:
    wcdigram::pao pb;
};


inline const std::string& WCDigram::key() const
{
    return pb.key();
}

inline const uint64_t WCDigram::count() const
{
    return pb.count();
}
