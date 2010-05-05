#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>


using namespace std;

void serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
{
	keyLength = keyLength + 1; /* write \0 */
	valueLength = valueLength + 1; /* write \0 */
	fwrite(&type, sizeof(char), 1, fileOut);
	fwrite(&keyLength, sizeof(uint64_t), 1, fileOut);
	fwrite(key, sizeof(char), keyLength, fileOut);
	fwrite(&valueLength, sizeof(uint64_t), 1, fileOut);
	fwrite(value, sizeof(char), valueLength, fileOut);
}

void deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
{
	fread(type, sizeof(char), 1, fileIn);
	fread(keyLength, sizeof(uint64_t), 1, fileIn);
	*key = (char*) malloc(*keyLength);
	fread(*key, sizeof(char), *keyLength, fileIn);
	fread(valueLength, sizeof(uint64_t), 1, fileIn);
	*value = (char*) malloc(*valueLength);
	fread(*value, sizeof(char), *valueLength, fileIn);
}

int main(int arg, char* args[])
{
	/* serialize some data */
	string key = "test key";
	string value = "test value";
	char type1 = 1;
	char key1[9] = {'t','e','s','t',' ','k','e','y','\0'};
	uint64_t keyLength1 = strlen(key1);
	char value1[11] = {'t','e','s','t',' ','v','a','l','u','e','\0'};
	uint64_t valueLength1 = strlen(value1);

	FILE* fileOut = fopen("serialized.out", "wb");

	serialize(fileOut, type1, (uint64_t) key.length(), key.c_str(), (uint64_t) value.length(), value.c_str());

	fclose(fileOut);

	/* deserialize some data */
	FILE* fileIn = fopen("serialized.out", "rb");
	char type2;
	char* key2;
	char* value2;
	uint64_t keyLength2;
	uint64_t valueLength2;

	deSerialize(fileIn, &type2, &keyLength2, &key2, &valueLength2, &value2);
	
	string keyout = string(key2);
	string valueout = string(value2);

	cout<<(int)type2<<" "<<keyout<<" "<<valueout<<"\n";

	printf("Type: %d\nKey Length: %lld\nKey: %s\nValue Length: %lld\nValue: %s\n", type2, keyLength2, key2, valueLength2, value2);

	/* free buffer memory */
	free(key2);
	free(value2);

	return EXIT_SUCCESS;
}
