# Glued together Makefiles, needs cleanup
all:
	cd master/ && make
	cd lib/ && make
	cd worker/gen-cpp && make

clean:
	cd master/ && make clean
	cd worker/gen-cpp && make clean
	cd lib/ && make clean
