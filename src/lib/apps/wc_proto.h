#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "Mapper.h"
#include "ProtobufPartialAgg.h"
#include "Tokenizer.h"
#include "wcproto.pb.h"

class WCProtoOperations : public ProtobufOperations
{
  public:
    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
    const char* getKey(PartialAgg* p) const;
	size_t createPAO(Token* t, PartialAgg** p) const;
    bool destroyPAO(PartialAgg* p) const;
	bool merge(PartialAgg* p, PartialAgg* mg) const;
    inline uint32_t getSerializedSize(PartialAgg* p) const;
    inline bool serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const;
    inline bool serialize(PartialAgg* p, std::string* output) const;
    inline bool serialize(PartialAgg* p, char* output, size_t size) const;
    inline bool deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) const;
    inline bool deserialize(PartialAgg* p, const std::string& input) const;
    inline bool deserialize(PartialAgg* p, const char* input,
            size_t size) const;
};

class WCProtoPAO : public PartialAgg {
    friend class WCProtoOperations;
  public:
	WCProtoPAO(char* wrd);
	~WCProtoPAO();
  private:
    wordcount::pao pb;
};