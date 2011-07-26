#include <stdlib.h>
#include <iostream>
#include "PartialAgg.h"

void* call_realloc(PartialAgg*** list, size_t siz)
{
	PartialAgg** tmp;
	if ((tmp = (PartialAgg**)realloc(*list, siz * sizeof(PartialAgg*))) == NULL) {
		perror("realloc failed");
		return NULL;
	}
	*list = tmp;
	return (void*)1;
}
