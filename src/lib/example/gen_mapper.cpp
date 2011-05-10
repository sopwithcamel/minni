#include "gen_mapper.h"
#include "stdlib.h"
#include "stdio.h"

TestMapperClass::~TestMapperClass () {
}

void TestMapperClass::Map(MapInput* input) {
	sleep(1);
}

int TestMapperClass::GetPartition (string key) {
	long key_id = atoi(key.c_str());
    return key_id % 10;
}

REGISTER_MAPPER(TestMapperClass);

