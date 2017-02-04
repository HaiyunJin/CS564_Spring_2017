##############################################################
#               CMake Project Wrapper Makefile               #
############################################################## 


CC=g++
SOURCES_=*.cpp exceptions/*.cpp 
SOURCES=$(addprefix ,$(SOURCES_))
SOURCES=$(SOURCES_)

CFLAGS=-Wall -Werror -I. -std=c++11


all:
	cd src;\
	$(CC) $(CFLAGS) $(SOURCES_) -o badgerdb_main


clean:
	cd src;\
	rm -f badgerdb_main test.? ../test.? *.o exceptions/*.o

doc:
	doxygen Doxyfile
