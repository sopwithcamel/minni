#include "PartialAgg.h"
#include <iostream>

using namespace std;

void PartialAgg::add(void* v)
{
}

void PartialAgg::merge (PartialAgg* add_agg)
{
}

void PartialAgg::serialize(FILE* f, void* buf)
{
}

bool PartialAgg::deserialize(FILE* f, void* buf)
{
}

bool PartialAgg::deserialize(void* buf)
{
}
