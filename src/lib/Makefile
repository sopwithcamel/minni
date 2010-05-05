HDFS=-I/home/abalacha/Map-Reduce-athula-code/hadoop-0.20.2/src/c++/libhdfs -L/home/abalacha/Map-Reduce-athula-code/hadoop-0.20.2/build/c++/Linux-i386-32/lib -I/usr/lib/jvm/java-6-sun/include -I/usr/lib/jvm/java-6-sun/include/linux -L/usr/lib/jvm/java-6-sun/jre/lib/i386/server -lhdfs -ljvm

all: PartialAgg.o Minnie.o Mapper.o Reducer.o 

PartialAgg.o: PartialAgg.cpp PartialAgg.h
	g++ -c PartialAgg.cpp


Minnie.o: Minnie.cpp Minnie.h PartialAgg.h
	g++ -c Minnie.cpp

Mapper.o: Mapper.cpp Mapper.h  
	g++ -c Mapper.cpp -I /usr/local/include/thrift/ -I gen-cpp/ -I master/ -I tbb/ $(HDFS)

Reducer.o: Reducer.cpp Reducer.h
	g++ -c Reducer.cpp -I /usr/local/include/thrift/ -I gen-cpp/ -I master/ -I tbb/ $(HDFS)

wordcount: wordcount.cpp 
	g++ wordcount.cpp -o wordcount


#Minnie.o: Minnie.cpp Minnie.h tbb/task.h tbb/tbb_thread.h 
#	g++ -c Minnie.cpp -I tbb30_20100406oss/include/tbb/ 

clean:
	rm *.o
