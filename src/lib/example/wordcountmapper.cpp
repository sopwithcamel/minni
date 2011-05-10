#include "wordcountmapper.h"

//Mapper
WordCountMapper::~WordCountMapper() {
//	for (int i = 0; i < my_file_streams.size(); i++)
  //      {
    //            delete my_file_streams[i];
      //  }
}

void WordCountMapper::Map(MapInput* input) {
	cout<<"Mapper: entered the map phase\n";
	cout<<"Mapper: I will be reading from KDFS soon\n";
	for (ChunkID id = input->chunk_id_start; id <= input->chunk_id_end; id++)
	{
		char* text;
		uint64_t n = input->key_value(&text,id);
		cout<<"Mapper: I have read from KDFS\n";
        	unsigned int i;
		for( i = 0; i < n; ) {
             		//skip through the leading whitespace
             		while((i < n) && isspace(text[i]))
        	          i++;
             		//Find word end
             		unsigned int start = i;
             		while ((i < n) && !isspace(text[i]))
                 		i++;
		
	     		if(start < i)
	     		{
				//cout<<"Mapper: The word is ";
				string key(&text[start],(i-start));
				//cout<<key;
				//cout<<endl;
				Emit(key,"1");
             		}
        	}	
		free(text);
		cout<<"Mapper: Done with map job\n";
	}
}

int WordCountMapper::GetPartition (string key) {//, int key_size) {
	unsigned long hash = 5381;
	char* str =  (char*) key.c_str();
	int key_size = key.length();
	int i;	
	for (i = 0; i < key_size; i++)
	{
		hash = ((hash << 5) + hash) + ((int) str[i]);
	}
	return hash % num_partition;
}

REGISTER_MAPPER(WordCountMapper);

