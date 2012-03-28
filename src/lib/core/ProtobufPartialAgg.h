#ifndef PROTOBUF_PARTIALAGG_H
#define PROTOBUF_PARTIALAGG_H
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "PartialAgg.h"

class Token;
class ProtobufPartialAgg : public PartialAgg {
  public:
	ProtobufPartialAgg() {}
	virtual ~ProtobufPartialAgg() {}

    bool usesProtobuf() const { return true; }

	/* serialize into file */
	virtual void serialize(
            google::protobuf::io::CodedOutputStream* output) const = 0;
	/* serialize into string */
	virtual void serialize(std::string* output) const = 0;
	/* deserialize from file */
	virtual bool deserialize(
            google::protobuf::io::CodedInputStream* input) = 0;
	/* deserialize from buffer */
	virtual bool deserialize(const std::string& input) = 0;
	virtual bool deserialize(const char* input, size_t size) = 0;
};

#endif