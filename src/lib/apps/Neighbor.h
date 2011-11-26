#ifndef APPS_NEIGHBOR_H
#define APPS_NEIGHBOR_H

class Neighbor {
  public:
	char* key;
	uint64_t distance;
	Neighbor()
	{
		key = NULL;
	}
	Neighbor(const Neighbor& n)
	{
		key = (char*)malloc(strlen(n.key) + 1);
		strcpy(key, n.key);
		distance = n.distance;
	}
	Neighbor(const char* k, size_t key_size)
	{
		key = (char*)malloc(key_size);
		strcpy(key, k);
	}
	Neighbor(const char* k, size_t key_size, float d)
	{
		key = (char*)malloc(key_size);
		strcpy(key, k);
		distance = d;
	}
	~Neighbor()
	{
		free(key);
	}
	static bool comp(Neighbor n1, Neighbor n2)
	{
		return (n1.distance < n2.distance);
	}
};

#endif // APPS_NEIGHBOR_H
