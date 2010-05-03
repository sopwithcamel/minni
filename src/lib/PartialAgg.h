#ifndef PartialAgg_H
#define PartialAgg_H
#include <string>
#include <sstream>

using namespace std;

class PartialAgg {
  public:
	PartialAgg();
	PartialAgg (string k, string v);
	~PartialAgg();
	virtual void add (string value);
	virtual void merge (PartialAgg* add_agg);
	string get_key();
	string get_value();
	void set_key(string k);
	void set_val(string v);

  private:
	string key;
	string value;
};



#endif


