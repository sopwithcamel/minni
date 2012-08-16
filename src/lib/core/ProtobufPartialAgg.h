#ifndef PROTOBUF_PARTIALAGG_H
#define PROTOBUF_PARTIALAGG_H
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "PartialAgg.h"

class Token;
class PbSerOperations : public Operations {
  public:
	PbSerOperations() {}
	virtual ~PbSerOperations() {}

    SerializationMethod getSerializationMethod() const { return PROTOBUF; }

    virtual uint32_t serializedSize() const = 0;

	/* serialize into file */
	virtual void serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const = 0;
	/* deserialize from file */
	virtual bool deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) = 0;
};

#endif
