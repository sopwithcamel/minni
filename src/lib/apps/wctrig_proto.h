#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "Mapper.h"
#include "ProtobufPartialAgg.h"
#include "Tokenizer.h"
#include "wcproto.pb.h"

class WCProtoOperations : public ProtobufOperations
{
  public:
    const char* getKey(PartialAgg* p) const;
    bool setKey(PartialAgg* p, char* k) const;
    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
	size_t createPAO(Token* t, PartialAgg** p) const;
    size_t dividePAO(const PartialAgg& p, PartialAgg** p_list) const;
    bool destroyPAO(PartialAgg* p) const;
	bool merge(PartialAgg* p, PartialAgg* mg) const;
    inline uint32_t getSerializedSize(PartialAgg* p) const;
    inline bool serialize(PartialAgg* p, std::string* output) const;
    inline bool serialize(PartialAgg* p, char* output, size_t size) const;
    inline bool deserialize(PartialAgg* p, const std::string& input) const;
    inline bool deserialize(PartialAgg* p, const char* input,
            size_t size) const;
    inline bool serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const;
    inline bool deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) const;
};

class WCTrigProtoPAO : public PartialAgg {
    friend class WCProtoOperations;
  public:
	WCTrigProtoPAO();
	~WCTrigProtoPAO();
  private:
    wordcount::pao pb;
};
