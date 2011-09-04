#include "PartialAgg.h"
#include <iostream>

using namespace std;

void PartialAgg::add(const char *v)
{
}


void PartialAgg::merge (PartialAgg* add_agg)
{
}

char* PartialAgg::get_value() {
	return value;
}


void PartialAgg::set_val(char* v) {
	strcpy(value, v);
}
