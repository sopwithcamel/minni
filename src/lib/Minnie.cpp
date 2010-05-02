#include <Minnie.h>

MapReduce::MapReduce(MapReduceSpecification spec, MapReduceResult* result)
{
	
}

//MapInput Class
virtual string MapInput::value() {
	//To Be Coded: should based on the file pointer/file path or offset etc read from HDFS
}


//Mapper class
virtual void Mapper::Emit (void* key, void* value) {
	//Writes intermediate key value pair to a local file
}

//MapperWrapper class

//changes to be made based on interface discussion
void MapperWrapper::set_file_pointer (FILE* fptr) {

}

void MapperWrapper::set_file_seek (int seek_pos) {

}

void MapperWrapper::set_num_bytes (int num_of_bytes) {

}

void MapperWrapper::start_mapper () {
	//check conditions for validity of inputs!

	my_mapper = create_fn();
	my_mapper->Map();
}

void MapperWrapper::set_output_file (string path) {

}

void MapperWrapper::set_creater (create_mapper_t* cfn) {

}

void MapperWrapper::set_destroyer (destroy_mapper_t* dfn) {

}

//ReduceInput class


//Reducer class
virtual void Reducer::Emit (void* key, void* value) {

}

//Reducer Wrapper
 

	



//MapReduceSpecification Functions
MapReduceInput* MapReduceSpecification::add_input () {

}

MapReduceOutput* MapReduceSpecification::output () {

}

void MapReduceSpecification::set_machines (int number)
{
	
}

int MapReduceSpecification::get_machines () {

}
		
void MapReduceSpecification::set_map_megabytes (int num_mb) {

}

int MapReduceSpecification::get_map_megabytes () {

}

void MapReduceSpecification::set_reduce_megabytes (int num_mb) {

}

int MapReduceSpecification::get_reduce_megabytes () {

}

void MapReduceSpecification::set_mapper_create_fn_ptr (create_mapper_t* create_mapper) {

}

void MapReduceSpecification::set_reducer_create_fn_ptr (create_reducer_t* create_reducer) {

}

void MapReduceSpecification::set_mapper_destoy_fn_ptr (destroy_mapper_t* destroy_mapper) {

}

void MapReduceSpecification::set_reducre_destroy_fn_ptr (destroy_reducer_t* destroy_reducer) {

}
		
