#ifndef BOOST_PARTIALAGG_H
#define BOOST_PARTIALAGG_H

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include "PartialAgg.h"

class Token;
class BoostPartialAgg : public PartialAgg {
  public:
	BoostPartialAgg() {}
	virtual ~BoostPartialAgg() {}

    SerializationMethod getSerializationMethod() const { return BOOST; }

	/* serialize into file */
	virtual void serialize(boost::archive::binary_oarchive* output) const = 0;
	/* deserialize from file */
	virtual bool deserialize(boost::archive::binary_iarchive* input) = 0;
};

#endif
