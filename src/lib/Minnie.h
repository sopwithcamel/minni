#ifndef Minnie_H
#define Minnie_H
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
//#include "tbb/task.h"
//#include "tbb/tbb_thread.h"

using namespace std;

class MapInput {
  public:
	MapInput() {};
	~MapInput() {};
	virtual string value(); // this is basically the input reader - full fledged implementation should enable overriding this!
  private:
	FILE* fptr;
	int seek;
	int num_bytes;
};

class Mapper {
  public:
	Mapper() {};
	~Mapper(){};
	virtual void Map (){} ; //will be overloaded
	virtual void Emit (void* key, void* value);
	
  private:
	string output_file_path;
};

/*class MapperWrapperTask: public task {
  public:
//	void set_file_pointer (FILE* fptr); //should be modified - discuss with Erik about interface!!
//	void set_file_seek (int seek_pos);
//	void set_num_bytes (int num_of_bytes);
//	void start_mapper ();        
//	void set_output_file (string path);
//	void set_creater (create_mapper_t* cfn);
//	void set_destroyer (destroy_mapper_t* dfn);	
	create_mapper_t* create_fn;
	destroy_mapper_t* destroy_fn;
	MapInput myinput;
	MapperWrapperTask (MapInput m_input);
	task* execute();
	

  //private:
//	MapInput my_input;
//	Mapper* my_mapper;
//	string output_file_path;
};*/



//the types of class factories
typedef Mapper* create_mapper_t();
typedef void destroy_mapper_t (Mapper*);

class ReduceInput { //to be decided and filled
  public:

  private:

};

class Reducer {
  public:
	virtual void Reduce ();
	virtual void Emit (void* key, void* value);

};

/*class ReducerWrapperTask : public task {
  public:
	//void start_reducer ();
	//void set_creater (create_reducer_t* cfn);
	//void set_destroyer (destroy_reducer_t* dfn);
	
	create_reducer_t* reduce_fn;
	destroy_reducer_t destroy_fn;	
  	ReduceInput my_input;
	ReducerWrapperTask (ReduceInput r_input);
	task* execute ();
  //private:
//	ReduceInput my_input;
//	Reducer* my_reducer;
};*/

//class factories
typedef Reducer* create_reducer_t();
typedef void destroy_reducer_t (Reducer*);


class MapReduceInput {
	public:
		void set_format (string type);
		string get_format ();
		void set_filepattern (string path); //I am assuming that this filepattern means the path - Check!
		string get_filepattern ();
	private:
		string file_format;
		string file_pattern;
};

struct MapReduceOutput {
	public:
		void set_filebase (string path);
		string get_filebase ();
		void set_num_tasks (int number);
		int get_num_tasks ();
		void set_format (string type);
		string get_format ();

	private:
		string filebase;
		int num_tasks;		
};


class MapReduceSpecification { //Talk to Wolf whether these are enough...
  public:
	MapReduceInput* add_input ();
	MapReduceOutput* output ();
	void set_machines (int number);
	int get_machines ();
	void set_map_megabytes (int num_mb);
	int get_map_megabytes ();
	void set_reduce_megabytes (int num_mb);
	int get_reduce_megabytes ();
	void set_mapper_create_fn_ptr (create_mapper_t* create_mapper);
	void set_reducer_create_fn_ptr (create_reducer_t* create_reducer);
	void set_mapper_destroy_fn_ptr (destroy_mapper_t* destroy_mapper);
	void set_reducer_destroy_fn_ptr (destroy_reducer_t* destroy_reducer);
	
	create_mapper_t* mapper_creater;
	create_reducer_t* reducer_creater;
	destroy_mapper_t* mapper_destroyer;
	destroy_reducer_t* reducer_destroyer;	

  private:
	MapReduceInput* array_inputs;
	int num_inputs;
	MapReduceOutput my_output;
	int num_machines;
	int map_megabytes;
	int reduce_megabytes;
		
};

class MapReduceResult { //Consult with Wolf abt what all to add
	public:
		//what all the results - ouput file name? and other statistics??

	private:
		//data

};

/*class MapReduce : public task{
  public:
	MapReduce(MapReduceSpecification input_spec, MapReduceResult* output_result);
	MapReduceSpecification my_spec;
	MapReduceResult *my_out;
	task* execute();
}*/



#endif //Minnie_H_
