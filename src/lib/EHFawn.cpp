#include "EHFawn.h"

/* Just initialize capacity, dumpNumber, dumpFile */
EHFawn::EHFawn(uint64_t cap, uint64_t pid)
{
	partid = pid;
	capacity = cap;
	dumpNumber = 0;	
	regularSerialize = true;
	evictHashName = "/localfs/hamur/fawnds_hashdump";
	evictHash = FawnDS<FawnDS_Flash>::Create_FawnDS(evictHashName.c_str(), 10000000, 0.9, 0.8, TEXT_KEYS);
	evictFile = fopen("/localfs/hamur/fawnds_test", "w");
	beg = hashtable.end();
	beg_ctr = 0;
	insert_ctr = 0;
	evict_ctr = 0;
	found_in_fds_ctr = 0;
}

/* perform finalize, send to default
 * filename (crash dump file) */
EHFawn::~EHFawn()
{
}

void EHFawn::setSerializeFormat(int sformat)
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
bool EHFawn::insert(string key, PartialAgg* pao)
{
//	cout << "Size " << hashtable.size() << ", cap: " << capacity << endl;
	if (hashtable.size() + 1 > capacity) {
//		cout << "Hit Hashtable capacity! " << endl;
		evict();
		evict_ctr++;
	}
	hashtable[key] = pao;
	return true;
}

bool EHFawn::add(const string &key, const string &value)
{
//	cout << "Key: " << key;
	Hash::iterator agg = hashtable.find(key);
	insert_ctr++;
	if (agg != hashtable.end()) {
//		cout << " found" << endl;
		agg->second->add(value);
	}
	else {
		insert(key, new PartialAgg(value));
//		cout << " inserted" << endl;
	}
	return true;
}

/* lookup in hashtable, if miss return error value
 * if hit, return corresponding PAO */
Hash::iterator EHFawn::find(const string &key)
{
	return hashtable.find(key);
}

void EHFawn::serialize(FILE* fileOut, string key, string value)
{
	static uint64_t syncCtr = 0;
        fwrite(key.c_str(), sizeof(char), key.size(), fileOut);
        fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), value.size(), fileOut);
        fwrite("\n", sizeof(char), 1, fileOut);
}

void EHFawn::serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
{
        keyLength = keyLength + 1; /* write \0 */
        valueLength = valueLength + 1; /* write \0 */
        fwrite(&type, sizeof(char), 1, fileOut);
        fwrite(&keyLength, sizeof(uint64_t), 1, fileOut);
        fwrite(key, sizeof(char), keyLength, fileOut);
        fwrite(&valueLength, sizeof(uint64_t), 1, fileOut);
        fwrite(value, sizeof(char), valueLength, fileOut);
}

/* 
 * Dump contents of FawnDS to the passed file
 */
bool EHFawn::finalize(string fname)
{
	
	FILE *f = fopen(fname.c_str(), "w");
	cout << "Dumping contents of FawnDS in finalize" << endl;
	set<string>::iterator it;
	char k[DBID_LENGTH];
	uint32_t key_length;
	bool valid, remove;
	char type = 1;
	string val;

	evictHash->split_init("");
	DBID se("a"); // some key
	while (evictHash->split_next(&se, &se, k, key_length, val, valid, remove)) {
		if (!valid) {
//			cout << k << " not valid" << endl;
			continue;
		}
		string key(k, key_length);
		cout << "dumping key " << key << ", " << val << endl;
//		assert(evictHash->Delete(k, strlen(k)));
		serialize(f, type, uint64_t (key.size()), key.c_str(), uint64_t (val.size()), val.c_str()); 
	}
	fclose(f);
}

void EHFawn::merge(Hash::iterator it)
{
	string ret_value;
        static int mctr = 0;
//	cout << "merge counter " << mctr++ << endl;

	if (evictHash->checkWriteBufFlush()) {
		tr1::unordered_map<string, bool>::iterator del_it;
//		cout << "Responding to flush..." << endl;
		for (del_it=bufferedKeys.begin(); del_it != bufferedKeys.end(); del_it++) {
			delete hashtable[del_it->first];
			hashtable.erase(del_it->first);
		}
		bufferedKeys.clear();
	}

	bufferedKeys[it->first] = true;
	string k = it->first;
	string val = it->second->value;
 
//	cout << "Trying to get " << k << endl;
	if (evictHash->Get(k.c_str(), k.size(), ret_value)) {

		char *tot = (char*)malloc(10);
		// ret_value needs to be added to val
		int _val = atoi(val.c_str());
		_val += atoi(ret_value.c_str());
		sprintf(tot, "%d", _val);
		(it->second->value).assign(tot);
		free(tot);
//		cout << "Found " << k << " in FDS; old value: " << ret_value << ", new value " << it->second->value << endl;
	}

//	cout << "Minni: Trying to insert " << k << ", " << it->second->value << endl;
	printf("Minni: it->first points to %p, %p\n", &(it->first), (it->first).c_str());

	assert(evictHash->Insert((it->first).c_str(), k.size(), (it->second->value).c_str(), (it->second->value).size()));
//	serialize(evictFile, 1, k.size(), k.c_str(), val.size(), val.c_str());
}

/*
 * Dump contents of hashtable to FawnDS
 */
bool EHFawn::finalize()
{
	Hash::iterator aggiter;
	for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
		if (bufferedKeys.find(aggiter->first) == bufferedKeys.end()) {
			merge(aggiter);
//			cout << "Inserting " << aggiter->first << ", " << aggiter->second->value << endl;
		}
	}
	fclose(evictFile);
	hashtable.clear();
}

bool EHFawn::evict()
{
	if (beg == hashtable.end()) {
		beg = hashtable.begin();
		beg_ctr++;
	}
        uint32_t ctr = 0;
	while (bufferedKeys.find(beg->first) != bufferedKeys.end()) {
//		cout << beg->first << " found in buf keys" << endl;
		beg++;
//		ctr++;
		if (beg == hashtable.end()) {
			beg = hashtable.begin();
			beg_ctr++;
		}
	}
//       cout << "trouble: " << ctr << endl;
	if (beg == hashtable.end())
		cout << "We're in trouble here!!" << endl;
	Hash::iterator aggiter = beg;
//	cout << "Eviction: Key " << aggiter->first << "with " << aggiter->second->value << " selected for eviction" << endl; 
	merge(aggiter);
//	delete aggiter->second;
//	hashtable.erase(aggiter);
}
	
bool EHFawn::clear()
{
	hashtable.clear();
}

int EHFawn::deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
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

