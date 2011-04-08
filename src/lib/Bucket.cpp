#include "Bucket.h"

uint64_t uniformPartition(char*);

/* Just initialize capacity, dumpNumber, dumpFile */
Bucket::Bucket(uint64_t cap, uint64_t pid)
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
	bucketFlag = true;
	emptyListTouched = PTHREAD_COND_INITIALIZER;
	touchLock = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
}

/* perform finalize, send to default
 * filename (crash dump file) */
Bucket::~Bucket()
{
}

uint64_t uniformPartition(char *k)
{
	int tot = 0, len = strlen(k);
	for (int i=0; i<len; i++)
		tot += k[i];
	return tot % EVICT_BUCKETS;
}

void Bucket::setSerializeFormat(int sformat)
{
	if (sformat == NSORT_SERIALIZE)
		regularSerialize = false;
	else
		regularSerialize = true;
}

void Bucket::setToMapOutput(string fname)
{
	mapOutFile = fopen(fname.c_str(), "w");
	if (mapOutFile == NULL) {
		perror("Unable to open file to dump hashtable to!\n");
	}
	bucketFlag = false;
}

/* if hashtable.size() + 1 == capacity
 * do dumpHashtable(fname) to default filename plus dumpNumber
 * insert <key,pao> into hashtable
 */
bool Bucket::insert(PartialAgg* pao)
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
bool Bucket::add(char* key, char* value)
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

void Bucket::serialize(FILE* fileOut, string key, string value)
{
	static uint64_t syncCtr = 0;
        fwrite(key.c_str(), sizeof(char), key.size(), fileOut);
        fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), value.size(), fileOut);
        fwrite("\n", sizeof(char), 1, fileOut);
}

//#define DEBUG_P

/*
 */
void Bucket::merge()
{
	string ret_value;
        static int mctr = 0;
// pick up empty list lock
 
	assert(!pthread_mutex_lock(&wakeupLock));
#ifdef DEBUG_P
	cout << "HRI: Cons: Picking up wakeup lock\n";
#endif
// wait for signal indicating there is work to do
wait_again:
#ifdef DEBUG_P
	cout << "HRI: Cons: Waiting for signal..." << endl;
#endif
	// wait for signal that there is a full list to empty
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
	while (*emptyListCtr > 0) {
		PartialAgg* pao = emptyList[EVICT_CACHE_SIZE - *emptyListCtr];
		char* k = pao->key;
		char* val = pao->value;

		if (bucketFlag)
			serialize(evictBucket[evictPartition(k)], string(k), string(val));
		else
			serialize(mapOutFile, string(k), string(val));
		free(val);
		free(k);
		delete pao;
		(*emptyListCtr)--;
	}
	// if the main thread is waiting for list to become empty, fire off a signal
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
	// Ok, to unlock only here because the cond_wait internally unlocks ...
	pthread_mutex_unlock(&wakeupLock);
	for (int i=0; i<EVICT_BUCKETS; i++)
		fclose(evictBucket[i]);
}

void Bucket::queueForMerge(PartialAgg* p)
{
	// pick up lock protecting fill list
	pthread_mutex_lock(fillLock);
#ifdef DEBUG_P
	cout << "HRI: Prod: Picking up fill lock at " << fillLock << endl;
#endif
	// check if the fill list is already full
	if (*fillListCtr == EVICT_CACHE_SIZE) {
#ifdef DEBUG_P
		cout << "HRI: Prod: Fill list full\n";
#endif
		pthread_mutex_lock(&touchLock);
		// if I/O thread is still emptying the other list, then wait
		// have while loop to handle false wakeups 
		while (touched == false)
			pthread_cond_wait(&emptyListTouched, &touchLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Picking up empty lock at " << emptyLock << endl;
#endif
		// Ok, the I/O thread has cleared out the empty list
		// so safe to switch pointers now

		// pick up other lock
		pthread_mutex_lock(emptyLock);
		// switch pointers
		PartialAgg** tmpList = fillList;
		fillList = emptyList;
		emptyList = tmpList;

		// switch counter pointers
		uint64_t* tmpCtr = fillListCtr;
		fillListCtr = emptyListCtr;
		emptyListCtr = tmpCtr; 

		// switch lock pointers
		pthread_mutex_t* tmpLock = fillLock;
		fillLock = emptyLock;
		emptyLock = tmpLock;

		// TODO: why set to false here?
		touched = false;
		pthread_mutex_unlock(&touchLock);

		// add to new fill list
		fillList[*fillListCtr] = p;
		(*fillListCtr)++;
		// release fill list lock first
		pthread_mutex_unlock(fillLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Releasing fill lock at " << fillLock << "; ctr: " << *fillListCtr << endl;
#endif

		pthread_mutex_unlock(emptyLock);
#ifdef DEBUG_P
		cout << "HRI: Prod: Releasing empty lock at " << emptyLock << endl;
#endif
		// time to wakeup I/O thread ...
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
bool Bucket::finalize()
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

bool Bucket::evict()
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
	
uint64_t Bucket::deserialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
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

