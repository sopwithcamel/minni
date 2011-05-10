#include "gen_pao.h"

void TestPaoClass::add (string v) {
     this->i++;
}

void TestPaoClass::merge (TestPaoClass* add_agg) {
     this->i += add_agg->i;
}

REGISTER_PAO(TestPaoClass);
