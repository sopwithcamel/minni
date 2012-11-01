#ifndef BOOST_PARTIALAGG_H
#define BOOST_PARTIALAGG_H

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <string.h>
#include "PartialAgg.h"

class Token;
class BoostOperations : public Operations {
  public:
	BoostOperations() {}
	virtual ~BoostOperations() {}

    SerializationMethod getSerializationMethod() const { return BOOST; }
    virtual void* getValue(PartialAgg* p) const {
        return NULL;
    }
    virtual void setValue(PartialAgg* p, void* v) const {
    }

	/* serialize into file */
	virtual bool serialize(PartialAgg* p,
            boost::archive::binary_oarchive* output) const = 0;
	/* deserialize from file */
	virtual bool deserialize(PartialAgg* p,
            boost::archive::binary_iarchive* input) const = 0;
};

#endif
