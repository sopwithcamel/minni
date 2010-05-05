PartialAgg.o: PartialAgg.cpp PartialAgg.h
	g++ -c PartialAgg.cpp


Minnie.o: Minnie.cpp Minnie.h PartialAgg.h
	g++ -c Minnie.cpp

Mapper.o: Mapper.cpp Mapper.h  
	g++ -c Mapper.cpp -I /usr/local/include/thrift/ -I gen-cpp/ -I master/ -I tbb/

Reducer.o: Reducer.cpp Reducer.h
	g++ -c Reducer.cpp -I /usr/local/include/thrift/ -I gen-cpp/ -I master/ -I tbb/

wordcount: wordcount.cpp 
	g++ wordcount.cpp -o wordcount


#Minnie.o: Minnie.cpp Minnie.h tbb/task.h tbb/tbb_thread.h 
#	g++ -c Minnie.cpp -I tbb30_20100406oss/include/tbb/ 

clean:
	rm *.o
