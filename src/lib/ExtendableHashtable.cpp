#include "config.h"
#include "ExtendableHashtable.h"

/* Just initialize capacity, dumpNumber, dumpFile */
ExtendableHashtable::ExtendableHashtable(uint64_t capacity, uint64_t partid)
{
	partid = partid;
	capacity = capacity;
	dumpNumber = 0;	
}

/* Initialize capacity, dumpNumber, dumpFile
 * Loop reading PAO's from file; use class insert
 * method to insert into hashtable */
ExtendableHashtable::ExtendableHashtable(uint64_t capacity, uint64_t partid, const string &path)
{
	partid = partid;
	capacity = capacity;
	dumpNumber = 0;
}

/* perform finalize, send to default
 * filename (crash dump file) */
ExtendableHashtable::~ExtendableHashtable()
{
}

/* if hashtable.size() + 1 == capacity
 * do dumpHashtable(fname) to default filename plus dumpNumber
 * insert <key,pao> into hashtable
 */
bool ExtendableHashtable::insert(string key, PartialAgg* pao)
{
	if (hashtable.size() + 1 == capacity) {
		string fname = getDumpFileName();
		dumpHashtable(fname);
		hashtable.clear();
	}
	hashtable[key] = pao;
	return true;
}

bool ExtendableHashtable::add(const string &key, const string &value)
{
	if (hashtable.find(key) == hashtable.end()) {
		insert(key, new PartialAgg(value));
	}
	else {
		hashtable[key]->add(value);
	}
	return true;
}

/* lookup in hashtable, if miss return error value
 * if hit, return corresponding PAO */
map<string,PartialAgg*>::iterator ExtendableHashtable::find(const string &key)
{
	return hashtable.find(key);
}

void ExtendableHashtable::serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
{
        keyLength = keyLength + 1; /* write \0 */
        valueLength = valueLength + 1; /* write \0 */
        fwrite(&type, sizeof(char), 1, fileOut);
        fwrite(&keyLength, sizeof(uint64_t), 1, fileOut);
        fwrite(key, sizeof(char), keyLength, fileOut);
        fwrite(&valueLength, sizeof(uint64_t), 1, fileOut);
        fwrite(value, sizeof(char), valueLength, fileOut);
}

/* essentially just dump the hashtable info the passed
 * path--user facing function (not internal helper)
 * call dumpHashtable */
bool ExtendableHashtable::finalize(FILE* fptr)
{
// For now, I just dump the entire hashtable in memory onto disk. Need to merge!
	map<string,PartialAgg*>::iterator aggiter;
	for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
		string k = aggiter->first;
		PartialAgg* curr_par = aggiter->second;
		string val = curr_par->value;
		char type = 1;
		serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
	}	
}

/* loop through PAO's in current hashtable writing to fname
 * according to merge logic *if* dumpFile is not empty string
 * meaning a dump file already exists, then also deserialize
 * loop through the existing dump file too properly merging
 * and saving serialized PAO's into fname */
bool ExtendableHashtable::dumpHashtable(string fname)
{
}

/* maintains state about current dumpfile version and returns 
 * the appropriate dumpfile name
 */
string ExtendableHashtable::getDumpFileName()
{
	char cCurrentPath[FILENAME_MAX];
	if (!GetCurrentDir(cCurrentPath, sizeof(cCurrentPath)))
	{
		return 0;  //TODO change here!!!
	}
	cCurrentPath[sizeof(cCurrentPath) - 1] = '\0'; /* not really required */
	string path = cCurrentPath;
	
	stringstream ss;
	ss << path;
	ss << "/dumpfile";
	ss << dumpNumber++;
	cout << "Local dumpfile (current): " << ss.str() << endl;
	return ss.str();
}

bool ExtendableHashtable::clear()
{
	hashtable.clear();
}

