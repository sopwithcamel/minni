#include "EHBucket.h"

uint64_t uniformPartition(char*);

/* Just initialize capacity, dumpNumber, dumpFile */
EHBucket::EHBucket(uint64_t cap, uint64_t pid)
{
	partid = pid;
	capacity = cap;
	dumpNumber = 0;	
	regularSerialize = true;
	for (int i=0; i < EVICT_BUCKETS; i++) {
		stringstream ss;
		ss << i;
		string evictBucketName = "/localfs/hamur/bucket" + ss.str();
		evictBucket[i] = fopen(evictBucketName.c_str(), "w");
	}
	evictPartition = uniformPartition;
	beg = hashtable.end();
	beg_ctr = 0;
	insert_ctr = 0;
	evict_ctr = 0;
	fds_read_ctr = 0;
// Create pthread to do I/O
	assert(!pthread_create(&evict_thread, NULL, merge_helper, this));
	for (int i=0; i<2; i++) {
		evictListCtr[i] = 0;
		evictListLock[i] = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
	}
	wakeupLock = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
	wakeup = false;
	fillList = evictList[0];
	emptyList = evictList[1];
	fillListCtr = &evictListCtr[0];
	emptyListCtr = &evictListCtr[1];
	fillLock = &evictListLock[0];
	emptyLock = &evictListLock[1];
	emptyListFull = PTHREAD_COND_INITIALIZER;
	exitThread = false;
	touched = true;
	emptyListTouched = PTHREAD_COND_INITIALIZER;
	touchLock = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
}

/* perform finalize, send to default
 * filename (crash dump file) */
EHBucket::~EHBucket()
{
}

uint64_t uniformPartition(char *k)
{
	int tot = 0, len = strlen(k);
	for (int i=0; i<len; i++)
		tot += k[i];
	return tot % EVICT_BUCKETS;
}

void EHBucket::setSerializeFormat(int sformat)
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
bool EHBucket::insert(PartialAgg* pao)
{
//	cout << "Size " << hashtable.size() << ", cap: " << capacity << endl;
	if (hashtable.size() + 1 > capacity) {
//		cout << "Hit Hashtable capacity! " << endl;
		evict();
		evict_ctr++;
	}
	hashtable[pao->key] = pao;
	return true;
}

/*
 * Memory has been allocated for the key in the tokenizing
 * already. It will be freed there as well.
 * We however need to allocate memory for the value from the 
 * heap. This will be freed when the object is evicted and 
 * flushed.
 */
bool EHBucket::add(char* key, char* value)
{
//	cout << "Key: " << key;
	Hash::iterator agg = hashtable.find(key);
	if (insert_ctr++ % 1000000 == 0) {
		char buffer[30];
		struct timeval tv;
		time_t curtime;
		gettimeofday(&tv, NULL); 
		curtime=tv.tv_sec;
		strftime(buffer,30,"%m-%d-%Y  %T.",localtime(&curtime));
		printf("%s%ld\n",buffer,tv.tv_usec);
		printf("Insert ctr: %llu\n", insert_ctr);
		printf("Evict ctr: %llu\n", evict_ctr);
	}
	if (agg != hashtable.end()) {
//		cout << " found" << endl;
		agg->second->add(value);
	}
	else {
//		cout << " created" << endl;
		char* new_key = (char*)malloc(strlen(key)+1);
//		char* new_key = (char*)malloc(30);
		char* new_val = (char*)malloc(10);
		strcpy(new_key, key);
		strcpy(new_val, value);
		PartialAgg* new_pao = new PartialAgg(new_key, new_val);
//		printf("New PAO created: key %s at %p, value at %p\n", new_pao->key, new_pao->key, new_pao->value);
		insert(new_pao);
	}
	return true;
}

void EHBucket::serialize(FILE* fileOut, string key, string value)
{
	static uint64_t syncCtr = 0;
        fwrite(key.c_str(), sizeof(char), key.size(), fileOut);
        fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), value.size(), fileOut);
        fwrite("\n", sizeof(char), 1, fileOut);
}

void EHBucket::serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
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
 * Dump contents of hashtable to the passed file
 */

bool EHBucket::finalize(string fname)
{	
	FILE *f = fopen(fname.c_str(), "w");
	Hash::iterator aggiter;
	char type = 1;
	for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
		char* k = aggiter->second->key;
		char* val = aggiter->second->value;

		serialize(f, type, strlen(k), k, strlen(val), val);
		free(k);
		free(val);
		delete aggiter->second;
	}
	hashtable.clear();
	fclose(f);
}


//#define DEBUG_P

/*
 * No need to free key here since it will be freed in the 
 * tokenizing code.
 * In this function,
 * 	Insert into FawnDS
 *	If the insert caused a flush
 *		Free elements in buffer
 * 		Reset pointer
 *	Add newly evicted element to the buffer
 */
void EHBucket::merge()
{
	string ret_value;
        static int mctr = 0;
// pick up empty list lock
 
	assert(!pthread_mutex_lock(&wakeupLock));
#ifdef DEBUG_P
	cout << "HRI: Cons: Picking up wakeup lock\n";
#endif
// wait for signal indicating I have work to do
wait_again:
#ifdef DEBUG_P
	cout << "HRI: Cons: Waiting for signal..." << endl;
#endif
	while (wakeup == false)
		assert(!pthread_cond_wait(&emptyListFull, &wakeupLock));
#ifdef DEBUG_P
	cout << "HRI: Cons: Woke up." << endl;
#endif
// Reset condition for next time
	emptyListFull = PTHREAD_COND_INITIALIZER;
	wakeup = false;
	assert(!pthread_mutex_lock(emptyLock));
#ifdef DEBUG_P
	cout << "HRI: Cons: Picking up empty list lock\n";
#endif
	char type = 1;
	while (*emptyListCtr > 0) {
		PartialAgg* pao = emptyList[EVICT_CACHE_SIZE - *emptyListCtr];
		char* k = pao->key;
		char* val = pao->value;

//		serialize(evictBucket[evictPartition(k)], type, strlen(k), k, strlen(val), val);
		serialize(evictBucket[evictPartition(k)], string(k), string(val));
		free(val);
		free(k);
		delete pao;
		(*emptyListCtr)--;
	}
	pthread_mutex_lock(&touchLock);
	touched = true;
	pthread_mutex_unlock(&touchLock);
	assert(!pthread_cond_signal(&emptyListTouched));
	assert(!pthread_mutex_unlock(emptyLock));
#ifdef DEBUG_P
	cout << "HRI: Cons: Releasing empty list lock\n";
#endif
	if (!exitThread)
		goto wait_again;
	pthread_mutex_unlock(&wakeupLock);
	for (int i=0; i<EVICT_BUCKETS; i++)
		fclose(evictBucket[i]);
}

void EHBucket::queueForMerge(PartialAgg* p)
{
	pthread_mutex_lock(fillLock);
#ifdef DEBUG_P
	cout << "HRI: Prod: Picking up fill lock at " << fillLock << endl;
#endif
	if (*fillListCtr == EVICT_CACHE_SIZE) {
#ifdef DEBUG_P
		cout << "HRI: Prod: Fill list full\n";
#endif
		pthread_mutex_lock(&touchLock);
		while (touched == false)
			pthread_cond_wait(&emptyListTouched, &touchLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Picking up empty lock at " << emptyLock << endl;
#endif
		pthread_mutex_lock(emptyLock);
		PartialAgg** tmpList = fillList;
		fillList = emptyList;
		emptyList = tmpList;

		uint64_t* tmpCtr = fillListCtr;
		fillListCtr = emptyListCtr;
		emptyListCtr = tmpCtr; 

		pthread_mutex_t* tmpLock = fillLock;
		fillLock = emptyLock;
		emptyLock = tmpLock;

		touched = false;
		pthread_mutex_unlock(&touchLock);

		fillList[*fillListCtr] = p;
		(*fillListCtr)++;
		pthread_mutex_unlock(fillLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Releasing fill lock at " << fillLock << "; ctr: " << *fillListCtr << endl;
#endif

		pthread_mutex_unlock(emptyLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Releasing empty lock at " << emptyLock << endl;
#endif
		pthread_mutex_lock(&wakeupLock);
		wakeup = true;
		pthread_mutex_unlock(&wakeupLock);
		assert(!pthread_cond_signal(&emptyListFull));
#ifdef DEBUG_P
		cout << "HRI: Prod: Sending signal to work!\n";
#endif
	} else {
		fillList[*fillListCtr] = p;
		(*fillListCtr)++;
		pthread_mutex_unlock(fillLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Releasing fill lock at " << fillLock << "; ctr: " << *fillListCtr << endl;
#endif
	}
}

/*
 * Dump contents of hashtable to FawnDS
 */
bool EHBucket::finalize()
{
	Hash::iterator aggiter;
	printf("hashtable size: %d\n", hashtable.size());
	for (aggiter = hashtable.begin(); aggiter != hashtable.end(); aggiter++) {
//		cout << "Dumping in finalize: " << aggiter->second->key << ", " << aggiter->second->value << endl;
		queueForMerge(aggiter->second);
	}
	exitThread = true;
	assert(!pthread_cond_signal(&emptyListFull));
	hashtable.clear();
}

bool EHBucket::evict()
{
	Hash::iterator lfu;
	int i = 0, lfu_val = INT_MAX;
	while (i < 1) {
		if (beg == hashtable.end()) {
			beg = hashtable.begin();
			beg_ctr++;
		}
		if (atoi(beg->second->value) < lfu_val) {
			lfu = beg;
			lfu_val = atoi(beg->second->value);
		}
		beg++;
		i++;
	}
//	cout << "Eviction: Key " << aggiter->first << "with " << aggiter->second->value << " selected for eviction" << endl; 
	queueForMerge(lfu->second);
//	cout << "Evicted" << endl;
	hashtable.erase(lfu);
}
	
uint64_t EHBucket::deserialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
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

