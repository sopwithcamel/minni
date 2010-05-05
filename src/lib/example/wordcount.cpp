#include "Mapper.h"
#include <stdlib.h>
#include <iostream>
#include <string>

using namespace std;
//User's map function
class WordCounter: public Mapper {
  public:
	virtual void Map (MapInput* input) {
		char* text;
		int n = input->key_value(&text);
			
		for(int i = 0; i < n; ) {
			//skip through the leading whitespace
			while((i < n) && isspace(text[i]))
				i++;
			
			//Find word end
			int start = i;
			while ((i < n) && !isspace(text[i]))
				i++;
			//if(start < i)
				//Emit();
		}
	}
}; //end class WordCounter
