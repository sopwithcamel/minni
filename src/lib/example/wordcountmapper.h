#include "Mapper.h"
#include "PartialAgg.h"

class WordCountMapper : public Mapper {
	public:
		void Map (MapInput* mapinp); //will be overloaded
		int GetPartition (string key); //the default parititioner. This can be overriden
		~WordCountMapper();
		WordCountMapper(PartialAgg* (*__libminni_create_pao)(string value)) : Mapper(__libminni_create_pao) {};

	private:
};
