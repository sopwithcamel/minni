#ifndef HANDSERLZPARTIALAGG_H
#define HANDSERLZPARTIALAGG_H

#include "PartialAgg.h"

class HandSerializedPartialAgg : public PartialAgg {
  public:
	HandSerializedPartialAgg() {}
	virtual ~HandSerializedPartialAgg() {}

    SerializationMethod getSerializationMethod() const { return HAND; }

	/* serialize into file */
	virtual void serialize(std::ofstream* output) const = 0;
	/* deserialize from file */
	virtual bool deserialize(std::ifstream* input) = 0;
};

#endif
