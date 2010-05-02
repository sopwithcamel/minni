#include "Minnie.h"
#include <stdlib.h>
#include <iostream>
#include <dlfcn.h>


//User's map function
class WordCounter: public Mapper {
  public:
	virtual void Map (const MapInput& input) {
		const string& text = input.value();	
		const int n = text.size();
		for(int i = 0; i < n; ) {
			//skip through the leading whitespace
			while((i < n) && isspace(text[i]))
				i++;
			
			//Find word end
			int start = i;
			while ((i < n) && !isspace(text[i]))
				i++;
			if(start < i)
				Emit(text.substr(start,i-start),"1");
		}
	}
}; //end class WordCounter
//REGISTER_MAPPER(WordCounter);

class Adder: public Reducer {
  public:
	virtual void Reduce (ReduceInput* input) {
		//Iterate over all entries with the same key and add all the values
		int64 value = 0;
		while(!input->done()) {
			value += StringToInt (input->value());
			input->NextValue()'
		}

		//Emit sum for input->key()
		Emit(IntToString(value));
	}
};//end class Adder
//REGISTER_REDUCER("Adder");

using namespace std;

int main(int argc, char** argv) {
	ParseCommandLineFlags(argc, argv);
	
	MapReduceSpecification spec;
	
	//Store the list of input files into "spec"
	for(int i = 1; i < argc; i++) {
		MapReduceInput* input = spec.add_input();
		input->set_format("text");
		input->set_filepattern(argv[i]);
		//input->set_mapper_class("WordCounter"); //needed?
	}

	//Specify the output files
	MapReduceOutput* out = spec.output();
	out->filebase("hdfs/test/freq");
	out->set_num_tasks(100);
	out->set_format("text");
	//out->set_reducer_class("Adder"); //needed?
	
	spec.set_machines(2000);
	spec.set_map_megabytes(100);
	spec.set_reduce_megabytes(100);
	//spec.set_mapper_class("WordCounter");
	//spec.set_reducer_class("Adder");
	//spec.set_shared_lib("libwordcount.so");

	//dynamically load classes
	void wordcount = dlopen("./libwordcount.so", RTLD_LAZY); //hardcoded for now
	if(!wordcount)	{
		cerr<<"Cannot load library: "<<dlerror() <<"\n";
		return EXIT_FAILURE;
	}
	
	//load the symbols
	create_mapper_t* create_wordcounter = (create_mapper_t*) dlsym(wordcount, "create");
	destroy_mapper_t* destroy_wordcounter = (destroy_t*) dlsym(wordcount, "destroy");
	create_reducer_t* create_adder = (create_reducer_t*) dlsym(wordcount, "create");
	destroy_reducer_t* destroy_adder = (destroy_reducer_t*) dlsym(wordcount,"destroy");

	if(!create_wordcounter || !destroy_wordcounter || !create_adder || !destroy_adder)
	{
		cout<<"Cannot load symbols: "<<dlerror()<<"\n";
		return EXIT_FAILURE;
	}

	spec.set_mapper_create_fn_ptr(create_wordcounter);
	spec.set_mapper_destroy_fn_ptr(destroy_wordcounter);
	spec.set_reducer_create_fn_ptr(create_adder);
	spec.set_reducer_destroy_fn_ptr(destroy_adder);

    	WordCounter my_word_counter;
	Adder my_adder;	
	
	spec.set_mapper_object(&my_word_counter);
	spec.set_reducer_object(&my_adder);

	MapReduceResult result;
	if(!MapReduce(spec, &result)) abort();

	return EXIT_SUCCESS;

} //end main


