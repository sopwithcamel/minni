#include "config.h"
#include "ExtendableHashtable.h"

/* Just initialize capacity, dumpNumber, dumpFile */
ExtendableHashtable::ExtendableHashtable(uint64_t cap, uint64_t pid)
{
	partid = pid;
	capacity = cap;
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
	cout << "Size " << hashtable.size() << ", cap: " << capacity << endl;
	if (hashtable.size() + 1 == capacity) {
		cout << "Hit Hashtable capacity!" << endl;
		string fname = getDumpFileName(dumpNumber);
		dumpHashtable(fname);
		dumpNumber++;
		hashtable.clear();
	}
	hashtable[key] = pao;
	return true;
}

bool ExtendableHashtable::add(const string &key, const string &value)
{
	if (hashtable.find(key) == hashtable.end()) {
		insert(key, new PartialAgg(value));
		cout << "Insert for new key " << key.c_str() << ", size " << hashtable.size() << endl;
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
 * according to merge logic *if* dumpNumber is not 0
 * meaning a dump file already exists, then also deserialize
 * loop through the existing dump file too properly merging
 * and saving serialized PAO's into fname */
bool ExtendableHashtable::dumpHashtable(string fname)
{
	FILE* fptr = fopen(fname.c_str(), "w");
	if (dumpNumber == 0) {
		map<string,PartialAgg*>::iterator aggiter;
		for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
			string k = aggiter->first;
			PartialAgg* curr_par = aggiter->second;
			string val = curr_par->value;
			char type = 1;
			serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
		}	
	}
	else {
		string prevDumpFile = getDumpFileName(dumpNumber);
		char *p_key, *p_value, type;
		uint64_t p_key_length, p_value_length;
		FILE* pdf = fopen(prevDumpFile.c_str(), "r");
		map<string,PartialAgg*>::iterator aggiter;
		aggiter = hashtable.begin();
		while (aggiter != hashtable.end() && feof(pdf) == 0) {
			string k = aggiter->first;
			if (deSerialize(pdf, &type, &p_key_length, &p_key, &p_value_length, &p_value)) break;
			if (type == 2) {
				cout << "Type is key-value. Should not be!";
			}
			if (k.compare(p_key) < 0) {
				PartialAgg* curr_par = aggiter->second;
				string val = curr_par->value;
				char type = 1;
				serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
				aggiter++;
			}
			else {
				char type = 1;
				serialize(fptr, type, p_key_length, p_key, p_value_length, p_value);
			}
			free(p_key);
			free(p_value);
		}
		while (aggiter != hashtable.end()) {
			string k = aggiter->first;
			PartialAgg* curr_par = aggiter->second;
			string val = curr_par->value;
			char type = 1;
			serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
			aggiter++;
		}
		while (feof(pdf)==0) {
			if (deSerialize(pdf, &type, &p_key_length, &p_key, &p_value_length, &p_value)) break;
			serialize(fptr, type, p_key_length, p_key, p_value_length, p_value);
			free(p_key);
			free(p_value);
		}
	}
}
	
/* returns the appropriate dumpfile name
 */
string ExtendableHashtable::getDumpFileName(uint64_t dn)
{
	stringstream ss;
	ss << "/localfs/hamur/dumpfile";
	ss << dn;
	cout << "Dumpfile: " << ss.str() << endl;
	return ss.str();
}

bool ExtendableHashtable::clear()
{
	hashtable.clear();
}

int ExtendableHashtable::deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
{
	size_t result;
	if (feof(fileIn)) return -1;
        result = fread(type, sizeof(char), 1, fileIn);
	if(result != 1) {cout<<"Reading error: TYPE "<<result<<endl; return -1;}
	if (feof(fileIn)) return -1;
        result = fread(keyLength, sizeof(uint64_t), 1, fileIn);
        if(result != 1) {cout<<"Reading error: KEY LEN " <<result<<endl; return -1;}
	if (feof(fileIn)) return -1;
        *key = (char*) malloc(*keyLength); /*freed line 136 after the particular loop run*/
	if (feof(fileIn)) return -1;
        result = fread(*key, sizeof(char), *keyLength, fileIn);
        if(result != *keyLength) {cout<<"Reading error: KEY VALUE "<<result<<endl; return -1;}
	if (feof(fileIn)) return -1;
        result = fread(valueLength, sizeof(uint64_t), 1, fileIn);
        if(result != 1) {cout<<"Reading error: VALUE LEN"<<result<<endl; return -1;}

	if (feof(fileIn)) return -1;
        *value = (char*) malloc(*valueLength); /*freed line 137 after the particular loop run*/
	if (feof(fileIn)) return -1;
        result = fread(*value, sizeof(char), *valueLength, fileIn);
        if(result != *valueLength){cout<<"Reading error: VALUE VALUE "<<result<<endl; return -1;}
	cout<<"Reducer: The values are key= "<<*key<<" value= "<<*value<<endl;
	return 0;
}

