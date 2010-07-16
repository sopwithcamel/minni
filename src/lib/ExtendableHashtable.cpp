#include "ExtendableHashtable.h"

/* Just initialize capacity, dumpNumber, dumpFile */
ExtendableHashtable::ExtendableHashtable(uint64_t cap, uint64_t pid)
{
	partid = pid;
	capacity = cap;
	dumpNumber = 0;	
	regularSerialize = true;
	evictFileName = getDumpFileName(dumpNumber);
	evictFile = fopen(evictFileName.c_str(), "w");
	beg = hashtable.end();
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
	static uint64_t hitcapctr = 0;
	if (hashtable.size() + 1 > capacity) {
//		cout << "Hit Hashtable capacity! " << hitcapctr++ << endl;
		evict();
	}
	hashtable[key] = pao;
	return true;
}

bool ExtendableHashtable::add(const string &key, const string &value)
{
//	cout << "Key: " << key << endl;
	Hash::iterator agg = hashtable.find(key);	
	if (agg == hashtable.end()) {
		insert(key, new PartialAgg(value));
//		cout << "inserted" << endl;
	}
	else {
		agg->second->add(value);
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
	static uint64_t syncCtr = 0;
        fwrite(key.c_str(), sizeof(char), key.size(), fileOut);
        fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), value.size(), fileOut);
        fwrite("\n", sizeof(char), 1, fileOut);
	if (syncCtr++ % 10000000 == 0) {
		syncCtr = 1;
//		fsync(fileOut);
		cout << "-------------------------------Syncing..." << endl;
	}
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
	if (evictFile == NULL)
		evictFile = fopen(evictFileName.c_str(), "ab");
	cout << "Calling dumpHashtable in finalize" << endl;
	dumpHashtable();
	fclose(evictFile);
	evictFile = NULL;
	if (rename(evictFileName.c_str(), fname.c_str())) {
		cout << "Error renaming file " << evictFileName << " to " << fname << endl;
	}
}

bool ExtendableHashtable::dumpHashtable()
{
	Hash::iterator aggiter;
	for (aggiter = hashtable.begin(); aggiter != hashtable.end();) {
		string k = aggiter->first;
		PartialAgg* curr_par = aggiter->second;
		string val = curr_par->value;
		char type = 1;
		if (regularSerialize)
			serialize(evictFile, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
		else
			serialize(evictFile, k, val); 
		Hash::iterator erase_element = aggiter++;
		delete erase_element->second;	
	}
	hashtable.clear();
}

inline bool ExtendableHashtable::evict()
{
	if (evictFile == NULL) {
		evictFile = fopen(evictFileName.c_str(), "ab");
		cout << "Opening evictfile again" << endl;
		beg = hashtable.end();
	}
//	unsigned long del_el = rand() % hashtable.size();
	if (beg == hashtable.end())
		beg = hashtable.begin();
	if (beg == hashtable.end())
		cout << "Hashtable empty, but not being caught!!" << endl;
	Hash::iterator aggiter = beg++;
//	advance(aggiter, del_el);
//	cout << "Key " << aggiter->first << "with " << aggiter->second->value << " deleted" << endl; 
	string k = aggiter->first;
	string val = aggiter->second->value;
	char type = 1;
	if (regularSerialize)
		serialize(evictFile, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
	else
		serialize(evictFile, k, val);
	delete aggiter->second;
	hashtable.erase(aggiter);
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

