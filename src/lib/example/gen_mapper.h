#include "Minnie.h"

class TestMapperClass : public Mapper {
	public:
		void Map (MapInput* mapinp);
		int GetPartition (string key);
		~TestMapperClass();
		TestMapperClass(PartialAgg* (*__libminni_create_pao)(string value)) : Mapper(__libminni_create_pao) {};

	private:
		 
};
