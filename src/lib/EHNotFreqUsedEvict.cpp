#include "EHNotFreqUsedEvict.h"

/* Just initialize capacity, dumpNumber, dumpFile */
EHNotFreqUsedEvict::EHNotFreqUsedEvict(uint64_t cap, uint64_t pid)
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
EHNotFreqUsedEvict::~EHNotFreqUsedEvict()
{
}

void EHNotFreqUsedEvict::setSerializeFormat(int sformat)
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
bool EHNotFreqUsedEvict::insert(string key, PartialAgg* pao)
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

bool EHNotFreqUsedEvict::add(const string &key, const string &value)
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
Hash::iterator EHNotFreqUsedEvict::find(const string &key)
{
	return hashtable.find(key);
}

void EHNotFreqUsedEvict::serialize(FILE* fileOut, string key, string value)
{
	static uint64_t syncCtr = 0;
        fwrite(key.c_str(), sizeof(char), key.size(), fileOut);
        fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), value.size(), fileOut);
        fwrite("\n", sizeof(char), 1, fileOut);
}

void EHNotFreqUsedEvict::serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
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
bool EHNotFreqUsedEvict::finalize(string fname)
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

bool EHNotFreqUsedEvict::dumpHashtable()
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

inline bool EHNotFreqUsedEvict::evict()
{
	if (evictFile == NULL) {
		evictFile = fopen(evictFileName.c_str(), "ab");
		cout << "Opening evictfile again" << endl;
		beg = hashtable.end();
	}
//	unsigned long del_el = rand() % hashtable.size();
	if (beg == hashtable.end())
		beg = hashtable.begin();
	Hash::iterator nfuiter = beg++;
	for (int j = 0; j < 30; j++) {
		if (beg == hashtable.end())
			beg = hashtable.begin();
		if (atoi(nfuiter->second->value.c_str()) > atoi(beg->second->value.c_str()))
			nfuiter = beg;
		beg++;
	}
//	advance(aggiter, del_el);
//	cout << "Key " << aggiter->first << "with " << aggiter->second->value << " deleted" << endl; 
	string k = nfuiter->first;
	string val = nfuiter->second->value;
	char type = 1;
	if (regularSerialize)
		serialize(evictFile, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
	else
		serialize(evictFile, k, val);
	delete nfuiter->second;
	hashtable.erase(nfuiter);
}
	
/* returns the appropriate dumpfile name
 */
string EHNotFreqUsedEvict::getDumpFileName(uint64_t dn)
{
	stringstream ss;
	ss << "/localfs/hamur/dumpfile";
	ss << dn;
	return ss.str();
}

bool EHNotFreqUsedEvict::clear()
{
	hashtable.clear();
}

int EHNotFreqUsedEvict::deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
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

