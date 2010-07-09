#include "config.h"
#include "ExtendableHashtable.h"

/* Just initialize capacity, dumpNumber, dumpFile */
ExtendableHashtable::ExtendableHashtable(uint64_t cap, uint64_t pid)
{
	partid = pid;
	capacity = cap;
	dumpNumber = 0;	
	regularSerialize = true;
	string fname = getDumpFileName(dumpNumber);
	FILE *fptr = fopen(fname.c_str(), "w");
	fclose(fptr);
}

/* perform finalize, send to default
 * filename (crash dump file) */
ExtendableHashtable::~ExtendableHashtable()
{
}

void ExtendableHashtable::setSerializeFormat(int sformat)
{
	if (sformat == NSORT_SERIALIZE)
		regularSerialize = false;
	else
		regularSerialize = true;
}

/* if hashtable.size() + 1 == capacity
 * do dumpHashtable(fname) to default filename plus dumpNumber
 * insert <key,pao> into hashtable
 */
bool ExtendableHashtable::insert(string key, PartialAgg* pao)
{
//	cout << "Size " << hashtable.size() << ", cap: " << capacity << endl;
	if (hashtable.size() + 1 > capacity) {
		cout << "Hit Hashtable capacity!" << endl;
		string fname = getDumpFileName(dumpNumber);
		dumpHashtable(fname);
	}
	hashtable[key] = pao;
	return true;
}

bool ExtendableHashtable::add(const string &key, const string &value)
{
	if (hashtable.find(key) == hashtable.end()) {
		insert(key, new PartialAgg(value));
//		cout << "Insert for new key " << key.c_str() << ", size " << hashtable.size() << endl;
	}
	else {
		hashtable[key]->add(value);
	}
	return true;
}

/* lookup in hashtable, if miss return error value
 * if hit, return corresponding PAO */
Hash::iterator ExtendableHashtable::find(const string &key)
{
	return hashtable.find(key);
}

void ExtendableHashtable::serialize(FILE* fileOut, string key, string value)
{
        fwrite(key.c_str(), sizeof(char), key.size(), fileOut);
        fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), value.size(), fileOut);
        fwrite("\n", sizeof(char), 1, fileOut);
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
bool ExtendableHashtable::finalize(string fname)
{
	string lastDumpfile = getDumpFileName(dumpNumber);
	dumpHashtable(lastDumpfile);
	if (rename(lastDumpfile.c_str(), fname.c_str())) {
		cout << "Error renaming file " << lastDumpfile << " to " << fname << endl;
	}
}

/* loop through PAO's in current hashtable writing to fname
 * according to merge logic *if* dumpNumber is not 0
 * meaning a dump file already exists, then also deserialize
 * loop through the existing dump file too properly merging
 * and saving serialized PAO's into fname */
bool ExtendableHashtable::dumpHashtable(string fname)
{
	FILE* fptr = fopen(fname.c_str(), "ab");
	Hash::iterator aggiter;
	for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
		cout << "Writing to " << fname << endl;
		string k = aggiter->first;
		PartialAgg* curr_par = aggiter->second;
		string val = curr_par->value;
		char type = 1;
		if (regularSerialize)
			serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
		else
			serialize(fptr, k, val); 
	}
	fclose(fptr);
	hashtable.clear();
}
	
/* returns the appropriate dumpfile name
 */
string ExtendableHashtable::getDumpFileName(uint64_t dn)
{
	stringstream ss;
	ss << "/localfs/hamur/dumpfile";
	ss << dn;
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
	cout << "---> Bytes read for type " << result << endl;
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

