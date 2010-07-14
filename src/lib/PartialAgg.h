#ifndef PartialAgg_H
#define PartialAgg_H
#include <string>
#include <sstream>
#include <map>
#include <stdlib.h>
#include <stdio.h>

using namespace std;

class PartialAgg {
  public:
	PartialAgg();
	PartialAgg (string v);
	~PartialAgg();
	virtual void add (string value);
	virtual void merge (PartialAgg* add_agg);
	string get_value();
	void set_val(string v);
	//string serialize(string key);

	string value;
};

#endif


