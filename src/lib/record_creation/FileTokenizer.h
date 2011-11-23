#ifndef LIB_FILETOKENIZER_H
#define LIB_FILETOKENIZER_H
#include "Tokenizer.h"
#include "MapInput.h"
#include <vector>
#include "Defs.h"

class FileTokenizer : public Tokenizer {
  public:
	FileTokenizer(MapInput* inp);
	~FileTokenizer();
	uint64_t getTokens(void*& data_fragments, uint64_t num_tokens,
			Token**& tokens);
  private:
	uint64_t num_tokens_sent;
	FileInput* input;
	vector<string> file_list;
};

#endif
