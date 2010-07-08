#include "config.h"
#include "AppendMap.h"

/* Just initialize capacity, dumpNumber, dumpFile */
AppendMap::AppendMap(uint64_t cap, uint64_t pid)
{
	partid = pid;
	capacity = cap;
	dumpNumber = 0;	
}

/* Initialize capacity, dumpNumber, dumpFile
 * Loop reading PAO's from file; use class insert
 * method to insert into hashtable */
AppendMap::AppendMap(uint64_t capacity, uint64_t partid, const string &path)
{
	partid = partid;
	capacity = capacity;
	dumpNumber = 0;
}

/* perform finalize, send to default
 * filename (crash dump file) */
AppendMap::~AppendMap()
{
}

/* if hashtable.size() + 1 == capacity
 * do dumpHashtable(fname) to default filename plus dumpNumber
 * insert <key,pao> into hashtable
 */
bool AppendMap::insert(string key, PartialAgg* pao)
{
	cout << "Size " << hashtable.size() << ", cap: " << capacity << endl;
	if (hashtable.size() + 1 > capacity) {
		cout << "Hit Hashtable capacity!" << endl;
		string fname = getDumpFileName(dumpNumber);
		dumpHashtable(fname);
		dumpNumber++;
		hashtable.clear();
	}
	hashtable[key] = pao;
	return true;
}

bool AppendMap::add(const string &key, const string &value)
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
map<string, PartialAgg*>::iterator AppendMap::find(const string &key)
{
	return hashtable.find(key);
}

void AppendMap::serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
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
bool AppendMap::finalize(string fname)
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
bool AppendMap::dumpHashtable(string fname)
{
	FILE* fptr = fopen(fname.c_str(), "wb");
	map<string, PartialAgg*>::iterator aggiter;
	for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
		cout << "Writing to " << fname << endl;
		string k = aggiter->first;
		PartialAgg* curr_par = aggiter->second;
		string val = curr_par->value;
		char type = 1;
		serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str()); 
	}
/*	
	}
	else {
		string prevDumpFile = getDumpFileName(dumpNumber - 1);
		cout << "Previous dump file " << prevDumpFile << endl;
		char *p_key, *p_value, type;
		uint64_t p_key_length, p_value_length;
		FILE* pdf = fopen(prevDumpFile.c_str(), "rb");
		unordered_map<string,PartialAgg*>::iterator aggiter;
		aggiter = hashtable.begin();
		PartialAgg tempPAO;
		bool readNextRecord = true;
		while (aggiter != hashtable.end() && feof(pdf) == 0) {
			string k = aggiter->first;
			if (readNextRecord) {
				if (deSerialize(pdf, &type, &p_key_length, &p_key, &p_value_length, &p_value)) break;
				readNextRecord = false;
			}
			cout << "type: " << type << " kl: " << p_key_length << " key: " << p_key << " vl: " << p_value_length << " val: " << p_value << endl;
			if (type == 2) {
				cout << "Type is key-value. Should not be!";
			}
			int keyDiff = k.compare(p_key);
			if (keyDiff < 0) {
				PartialAgg* curr_par = aggiter->second;
				string val = curr_par->value;
				char type = 1;
				serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
				aggiter++;
			}
			else if (keyDiff == 0) {
				tempPAO.set_val(p_value);
				aggiter->second->merge(&tempPAO);
				string val = aggiter->second->value;
				char type = 1;
				serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
				aggiter++;
				readNextRecord = true;
			}
			else {
				char type = 1;
				serialize(fptr, type, p_key_length - 1, p_key, p_value_length - 1, p_value);
				free(p_key);
				free(p_value);
				readNextRecord = true;
			}
			
		}
		while (aggiter != hashtable.end()) {
			string k = aggiter->first;
			PartialAgg* curr_par = aggiter->second;
			string val = curr_par->value;
			char type = 1;
			serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
			aggiter++;
		}
		if (!readNextRecord) {
			char type = 1;
			serialize(fptr, type, p_key_length - 1, p_key, p_value_length - 1, p_value);
			free(p_key);
			free(p_value);
			readNextRecord = true;
		}
		while (feof(pdf)==0) {
			if (deSerialize(pdf, &type, &p_key_length, &p_key, &p_value_length, &p_value)) break;
			cout << "type: " << type << " kl: " << p_key_length << " key: " << p_key << " vl: " << p_value_length << " val: " << p_value << endl;
			serialize(fptr, type, p_key_length - 1, p_key, p_value_length - 1, p_value);
			free(p_key);
			free(p_value);
		}
		fclose(pdf);
	}
*/
	fclose(fptr);
}
	
/* returns the appropriate dumpfile name
 */
string AppendMap::getDumpFileName(uint64_t dn)
{
	stringstream ss;
	ss << "/localfs/hamur/dumpfile";
	ss << dn;
	cout << "Dumpfile: " << ss.str() << endl;
	return ss.str();
}

bool AppendMap::clear()
{
	hashtable.clear();
}

int AppendMap::deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
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

