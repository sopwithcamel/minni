#include "config.h"
#include "ExtendableHashtable.h"

/* Just initialize capacity, dumpNumber, dumpFile */
ExtendableHashtable::ExtendableHashtable(uint64_t capacity)
{
}

/* Initialize capacity, dumpNumber, dumpFile
 * Loop reading PAO's from file; use class insert
 * method to insert into hashtable */
ExtendableHashtable::ExtendableHashtable(uint64_t capacity, const string &path)
{
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
PartialAgg ExtendableHashtable::insert(string key, PartialAgg pao)
{
}

/* lookup in hashtable, if miss return error value
 * if hit, return corresponding PAO */
PartialAgg ExtendableHashtable::get(const string &key)
{
}

/* essentially just dump the hashtable info the passed
 * path--user facing function (not internal helper)
 * call dumpHashtable */
bool ExtendableHashtable::finalize(const string &path)
{
}

/* loop through PAO's in current hashtable writing to fname
 * according to merge logic *if* dumpFile is not empty string
 * meaning a dump file already exists, then also deserialize
 * loop through the existing dump file too properly merging
 * and saving serialized PAO's into fname */
bool ExtendableHashtable::dumpHashtable(string fname)
{
}
