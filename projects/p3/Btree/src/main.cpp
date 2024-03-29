
// #define DEBUG
// #define DEBUGMORE
// #define DEBUGSTRING
// #define DEBUGSCAN
// #define DEBUGPRINTTREE

/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <vector>
#include "btree.h"
#include "page.h"
#include "filescan.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/end_of_file_exception.h"

// #include "exceptions/file_open_exception.h"
#include "exceptions/empty_btree_exception.h"



#define checkPassFail(a, b)	                                            \
{                                       								\
	if(a == b)											                \
		std::cout << "\nTest passed at line no:" << __LINE__ << "\n";	\
	else															    \
	{																	\
		std::cout << "\nTest FAILS at line no:" << __LINE__;	        \
		std::cout << "\nExpected no of records:" << b;					\
		std::cout << "\nActual no of records found:" << a;			    \
		std::cout << std::endl;											\
		exit(1);														\
	}																	\
}


using namespace badgerdb;

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------
int testNum = 1;
const std::string relationName = "relA";
//If the relation size is changed then the second parameter 2 chechPassFail
//may need to be changed to number of record that are expected to be found 
//during the scan, else tests will erroneously be reported to have failed.
const int	relationSize = 5000;
std::string intIndexName, doubleIndexName, stringIndexName;

// This is the structure for tuples in the base relation

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;

PageFile* file1;
RecordId rid;
RECORD record1;
std::string dbRecord1;

BufMgr * bufMgr = new BufMgr(100);

// -----------------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------------

void createRelationForward();
void createRelationForward(int size);
void createRelationBackward();
void createRelationBackward(int size);
void createRelationRandom();
void createRelationRandom(int size);
void createRelationLarge(std::string mode, int size);
void intTests();
void intTestsnonleaf();
int intScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void indexTests();
void indexTests2();
void doubleTests();
void doubleTestsnonleaf();
int doubleScan(BTreeIndex *index, double lowVal, Operator lowOp, double highVal, Operator highOp);
void stringTests();
void stringTestsnonleaf();
int stringScanLarge(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
int stringScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void test1();
void test2();
void test3();
void test4();
void test5();
void test55();
void test6();
void test7(); // duplicate key
void test8(); // test delete
void test9(); // test delete
void test10(); // test delete
void test11(); // test delete
void test12(); // test delete
void errorTests();
void deleteRelation();

int main(int argc, char **argv)
{
	if( argc != 2 )
	{
		std::cout << "Expects one argument as a number between 1 to 3 to choose datatype of key.\n";
		std::cout << "For INTEGER keys run as: ./badgerdb_main 1\n";
		std::cout << "For DOUBLE keys run as: ./badgerdb_main 2\n";
		std::cout << "For STRING keys run as: ./badgerdb_main 3\n";
		delete bufMgr;
		return 1;
	}

	sscanf(argv[1],"%d",&testNum);

	switch(testNum)
	{
		case 1:
			std::cout << "leaf size:" << INTARRAYLEAFSIZE << " non-leaf size:" << INTARRAYNONLEAFSIZE << std::endl;
			break;
		case 2:
			std::cout << "leaf size:" << DOUBLEARRAYLEAFSIZE << " non-leaf size:" << DOUBLEARRAYNONLEAFSIZE << std::endl;
			break;
		case 3:
			std::cout << "leaf size:" << STRINGARRAYLEAFSIZE << " non-leaf size:" << STRINGARRAYNONLEAFSIZE << std::endl;
			break;
	}


  // Clean up from any previous runs that crashed.
  try {
    File::remove(relationName);
  } catch(FileNotFoundException) {
  }

	{
		// Create a new database file.
		PageFile new_file = PageFile::create(relationName);

		// Allocate some pages and put data on them.
		for (int i = 0; i < 20; ++i)
		{
			PageId new_page_number;
			Page new_page = new_file.allocatePage(new_page_number);

            sprintf(record1.s, "%05d string record", i);
            record1.i = i;
            record1.d = (double)i;
            std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			new_page.insertRecord(new_data);
			new_file.writePage(new_page_number, new_page);
		}

	}
	// new_file goes out of scope here, so file is automatically closed.

	{
		FileScan fscan(relationName, bufMgr);

		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int *)(record + offsetof (RECORD, i)));
				std::cout << "Extracted : " << key << std::endl;
			}
		}
		catch(EndOfFileException e)
		{
			std::cout << "Read all records" << std::endl;
		}
	}
	// filescan goes out of scope here, so relation file gets closed.

	File::remove(relationName);

	test1();
	test2();
	test3();
    {
      errorTests();
      try { File::remove(intIndexName);} catch(FileNotFoundException e) { }
    }

    // haiyun add
    test4(); // test read old file
    test5(); // test split non-leaf file, small page
    test55(); // test split non-leaf file, large entries
    test6(); // test read existing but bad file
//     test7(); // test duplicate key // not working
  
    test8(); // test delete: delete an entry
    test9(); // test delete: delete entries until redistribute
    test10(); // test delete: delete until merge leaf
    test11(); // test delete: delete until merge non leaf
    test12(); // test delete: delete from an empty tree

#ifdef DEBUG
  std::cout<< "in main before delete bufMgr"<< std::endl;
#endif
	delete bufMgr;
#ifdef DEBUG
  std::cout<< "in main after delete bufMgr"<< std::endl;
#endif
	return 0;
}

void test1()
{
	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationForward();
	indexTests();
	deleteRelation();
}

void test2()
{
	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationBackward();
	indexTests();
	deleteRelation();
}

void test3()
{
	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationRandom();
	indexTests();
	deleteRelation();
}


void test4()
{
	std::cout << std::endl;
	std::cout << std::endl;
	std::cout << "----------------------" << std::endl;
	std::cout << "- test read old file -" << std::endl;
	std::cout << "----------------------" << std::endl;
	std::cout << std::endl;
	std::cout << std::endl;

	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationForward();
	indexTests2();
	deleteRelation();

	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationBackward();
	indexTests2();
	deleteRelation();

	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationRandom();
	indexTests2();
	deleteRelation();
}

void test5()
{

    const int size = 130000; // 340*340

	std::cout << std::endl;
	std::cout << std::endl;
	std::cout << "----------------------" << std::endl;
	std::cout << "- test split non-leaf node -" << std::endl;
	std::cout << "----------------------" << std::endl;
	std::cout << std::endl;
	std::cout << std::endl;

	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationForward(size);
	indexTests();
	deleteRelation();

	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationBackward(size);
	indexTests();
	deleteRelation();

	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationRandom(size);
	indexTests();
	deleteRelation();
}

void test55()
{

    const int size = 500000; // 340*340*2

	std::cout << std::endl;
	std::cout << std::endl;
	std::cout << "---------------------------------------" << std::endl;
	std::cout << "- test split non-leaf node scan large -" << std::endl;
	std::cout << "---------------------------------------" << std::endl;
	std::cout << std::endl;
	std::cout << std::endl;

	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationLarge("forward", size);

    intTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
    doubleTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
    stringTestsnonleaf();
	try {
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
		File::remove(intIndexName);
	} catch(FileNotFoundException e) {
  	}
	deleteRelation();

	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationLarge("backward", size);
    intTestsnonleaf();
	try {
		File::remove(intIndexName);
	} catch(FileNotFoundException e) {
  	}
    doubleTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
    stringTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
// 	indexTests();
	deleteRelation();

	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationLarge("random", size);
    intTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
    doubleTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
    stringTestsnonleaf();
	try {
		File::remove(intIndexName);
		File::remove(doubleIndexName);
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
	deleteRelation();
}


void test6()
{
	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	createRelationForward();
    
//     try
    {
      std::cout << "  Treat as integer   " << std::endl;
      BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
    }
    {
      std::cout << "  Treat as double   " << std::endl;
      BTreeIndex index2(relationName, doubleIndexName, bufMgr, offsetof(tuple,i), DOUBLE);

    }
    try {
        File::remove(intIndexName);
        File::remove(doubleIndexName);
    } catch(FileNotFoundException e) {
    }
    deleteRelation();


}






// -----------------------------------------------------------------------------
// createRelationLarge
// -----------------------------------------------------------------------------

void createRelationLarge( std::string mode, int size )
{

  if ( mode.compare("forward") == 0 ) {

	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
  PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < size; i++ )
	{
    sprintf(record1.s, "%010d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
            catch(InsufficientSpaceException e)
            {
              file1->writePage(new_page_number, new_page);
              new_page = file1->allocatePage(new_page_number);
            }
		}
  }

	file1->writePage(new_page_number, new_page);

  } else if ( mode.compare("backward") == 0 ) {
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = size - 1; i >= 0; i-- )
	{
    sprintf(record1.s, "%010d string record", i);
    record1.i = i;
    record1.d = i;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);

  } else if ( mode.compare("random") == 0 ) {
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // insert records in random order

  std::vector<int> intvec(size);
  for( int i = 0; i < size; i++ )
  {
    intvec[i] = i;
  }

  long pos;
  int val;
	int i = 0;
  while( i < size ) {
    pos = random() % (size-i);
    val = intvec[pos];
    sprintf(record1.s, "%010d string record", val);
    record1.i = val;
    record1.d = val;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

    while(1) {
      try {
        new_page.insertRecord(new_data);
        break;
      } catch(InsufficientSpaceException e)
      {
        file1->writePage(new_page_number, new_page);
        new_page = file1->allocatePage(new_page_number);
      }
    }

    int temp = intvec[size-1-i];
    intvec[size-1-i] = intvec[pos];
    intvec[pos] = temp;
    i++;
  }
  
	file1->writePage(new_page_number, new_page);
  }

}





// -----------------------------------------------------------------------------
// createRelationForward
// -----------------------------------------------------------------------------

void createRelationForward()
{
    createRelationForward(relationSize);
}

void createRelationForward(int size)
{
	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
  PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < size; i++ )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
            catch(InsufficientSpaceException e)
            {
              file1->writePage(new_page_number, new_page);
              new_page = file1->allocatePage(new_page_number);
            }
		}
  }

	file1->writePage(new_page_number, new_page);
}


// -----------------------------------------------------------------------------
// createRelationBackward
// -----------------------------------------------------------------------------

void createRelationBackward()
{
  createRelationBackward(relationSize);
}

void createRelationBackward(int size)
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = size - 1; i >= 0; i-- )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = i;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}



// -----------------------------------------------------------------------------
// createRelationRandom
// -----------------------------------------------------------------------------

void createRelationRandom()
{
  createRelationRandom(relationSize);
}

void createRelationRandom(int size)
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // insert records in random order

  std::vector<int> intvec(size);
  for( int i = 0; i < size; i++ )
  {
    intvec[i] = i;
  }

  long pos;
  int val;
	int i = 0;
  while( i < size ) {
    pos = random() % (size-i);
    val = intvec[pos];
    sprintf(record1.s, "%05d string record", val);
    record1.i = val;
    record1.d = val;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

    while(1) {
      try {
        new_page.insertRecord(new_data);
        break;
      } catch(InsufficientSpaceException e)
      {
        file1->writePage(new_page_number, new_page);
        new_page = file1->allocatePage(new_page_number);
      }
    }

    int temp = intvec[size-1-i];
    intvec[size-1-i] = intvec[pos];
    intvec[pos] = temp;
    i++;
  }
  
	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// indexTests
// -----------------------------------------------------------------------------

void indexTests()
{
#ifdef DEBUG
  std::cout<<"Starting indexTest!!!!!!!!!!!!!!!\n";
#endif
  if(testNum == 1)
  {
    intTests();
	try {
		File::remove(intIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
  else if(testNum == 2)
  {
    doubleTests();
	try {
		File::remove(doubleIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
  else if(testNum == 3)
  {
    stringTests();
	try {
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
}



// -----------------------------------------------------------------------------
// indexTests2
// -----------------------------------------------------------------------------

void indexTests2()
{
#ifdef DEBUG
  std::cout<<"Starting indexTest2!!!!!!!!!!!!!!!\n";
#endif
  if(testNum == 1)
  {
    intTests();
    intTests();
	try {
		File::remove(intIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
  else if(testNum == 2)
  {
    doubleTests();
    doubleTests();
	try {
		File::remove(doubleIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
  else if(testNum == 3)
  {
    stringTests();
    stringTests();
	try {
		File::remove(stringIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
}




// -----------------------------------------------------------------------------
// intTests
// -----------------------------------------------------------------------------

void intTests()
{
  std::cout << "Create a B+ Tree index on the integer field" << std::endl;
  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

#ifdef DEBUG
// haiyun
  std::cout<<intIndexName<<std::endl;
#endif
	// run some tests
	checkPassFail(intScan(&index,25,GT,40,LT), 14)
	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(&index,-3,GT,3,LT), 3)
	checkPassFail(intScan(&index,338,GT,1001,LT), 1001-338-1)
	checkPassFail(intScan(&index,339,GT,1001,LT), 1001-339-1)
	checkPassFail(intScan(&index,340,GT,1001,LT), 1001-340-1)
	checkPassFail(intScan(&index,341,GT,1001,LT), 1001-341-1)
	checkPassFail(intScan(&index,342,GT,1001,LT), 1001-342-1)
	checkPassFail(intScan(&index,338,GTE,1001,LT), 1001-338)
	checkPassFail(intScan(&index,339,GTE,1001,LT), 1001-339)
	checkPassFail(intScan(&index,340,GTE,1001,LT), 1001-340)
	checkPassFail(intScan(&index,341,GTE,1001,LT), 1001-341)
	checkPassFail(intScan(&index,342,GTE,1001,LT), 1001-342)
	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
	checkPassFail(intScan(&index,0,GT,1,LT), 0)
	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,4000,LT), 1000)
	checkPassFail(intScan(&index,4950,GTE,5000,LT), 50) // added
}


// -----------------------------------------------------------------------------
// intTests customized
// -----------------------------------------------------------------------------

void intTestsnonleaf()
{
  std::cout << "Create a B+ Tree index on the integer field" << std::endl;
  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

#ifdef DEBUG
// haiyun
  std::cout<<intIndexName<<std::endl;
#endif
	// run some tests
	checkPassFail(intScan(&index,25,GT,40,LT), 14)
// 	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
// 	checkPassFail(intScan(&index,-3,GT,3,LT), 3)
// 	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
// 	checkPassFail(intScan(&index,0,GT,1,LT), 0)
// 	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,400000,LT), 397000)
}





int intScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  int numResults = 0;
	
  try
  {
#ifdef DEBUGMORE
    std::cout<<" start intTest startScan haiyun"<<std::endl;
#endif
    index->startScan(&lowVal, lowOp, &highVal, highOp);
#ifdef DEBUGMORE
    std::cout<<" end of intTest startScan haiyun"<<std::endl;
#endif
  }
  catch(NoSuchKeyFoundException e)
  {
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
    return 0;
  }

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
#ifdef DEBUG
  std::cout<<" start intScan.cpp:"<<__LINE__<<std::endl;
  std::cout<<" scanRid.page_number is "<< scanRid.page_number<<std::endl;
#endif
			bufMgr->readPage(file1, scanRid.page_number, curPage);
#ifdef DEBUG
    std::cout<<" cal Page::getRecord at "<<__LINE__<<std::endl;
#endif
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "at:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
#ifdef DEBUGSCAN
  std::cout<<"scan complete\n";
#endif
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

void doubleTestsnonleaf()
{
  std::cout << "Create a B+ Tree index on the double field" << std::endl;
  BTreeIndex index(relationName, doubleIndexName, bufMgr, offsetof(tuple,d), DOUBLE);
#ifdef DEBUG
  std::cout << "FINISHED CREATING INDEX FILE" << std::endl;
#endif

	// run some tests
	checkPassFail(doubleScan(&index,25,GT,40,LT), 14)
// 	checkPassFail(doubleScan(&index,20,GTE,35,LTE), 16)
// 	checkPassFail(doubleScan(&index,-3,GT,3,LT), 3)
// 	checkPassFail(doubleScan(&index,996,GT,1001,LT), 4)
// 	checkPassFail(doubleScan(&index,0,GT,1,LT), 0)
// 	checkPassFail(doubleScan(&index,300,GT,400,LT), 99)
	checkPassFail(doubleScan(&index,3000,GTE,400000,LT), 397000)
}

// -----------------------------------------------------------------------------
// doubleTests
// -----------------------------------------------------------------------------

void doubleTests()
{
  std::cout << "Create a B+ Tree index on the double field" << std::endl;
  BTreeIndex index(relationName, doubleIndexName, bufMgr, offsetof(tuple,d), DOUBLE);
#ifdef DEBUG
  std::cout << "FINISHED CREATING INDEX FILE" << std::endl;
#endif

	// run some tests
	checkPassFail(doubleScan(&index,25,GT,40,LT), 14)
	checkPassFail(doubleScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(doubleScan(&index,-3,GT,3,LT), 3)
	checkPassFail(doubleScan(&index,253,GT,1001,LT), 1001-253-1)
	checkPassFail(doubleScan(&index,254,GT,1001,LT), 1001-254-1)
	checkPassFail(doubleScan(&index,255,GT,1001,LT), 1001-255-1)
	checkPassFail(doubleScan(&index,256,GT,1001,LT), 1001-256-1)
	checkPassFail(doubleScan(&index,257,GT,1001,LT), 1001-257-1)
	checkPassFail(doubleScan(&index,253,GTE,1001,LT), 1001-253)
	checkPassFail(doubleScan(&index,254,GTE,1001,LT), 1001-254)
	checkPassFail(doubleScan(&index,255,GTE,1001,LT), 1001-255)
	checkPassFail(doubleScan(&index,256,GTE,1001,LT), 1001-256)
	checkPassFail(doubleScan(&index,257,GTE,1001,LT), 1001-257)
	checkPassFail(doubleScan(&index,996,GT,1001,LT), 4)
	checkPassFail(doubleScan(&index,0,GT,1,LT), 0)
	checkPassFail(doubleScan(&index,300,GT,400,LT), 99)
	checkPassFail(doubleScan(&index,3000,GTE,4000,LT), 1000)
}

int doubleScan(BTreeIndex * index, double lowVal, Operator lowOp, double highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  int numResults = 0;

	try
	{
  	index->startScan(&lowVal, lowOp, &highVal, highOp);
	}
	catch(NoSuchKeyFoundException e)
	{
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
		return 0;
	}

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
#ifdef DEBUG
  std::cout<<" start doubleScan.cpp:"<<__LINE__<<std::endl;
  std::cout<<" scanRid.page_number is "<< scanRid.page_number<<std::endl;
#endif
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "rid:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}


// -----------------------------------------------------------------------------
// stringTestsnonleaf
// -----------------------------------------------------------------------------

void stringTestsnonleaf()
{
  std::cout << "Create a B+ Tree index on the string field" << std::endl;
  BTreeIndex index(relationName, stringIndexName, bufMgr, offsetof(tuple,s), STRING);

	// run some tests
#ifdef DEBUGSTRING
    int out = stringScan(&index,25,GT,40,LT);
std::cout<<" stringScan(&index,25,GT,40,LT); RESULT is "<<out<<std::endl;
	checkPassFail(out, 14)
#else
	checkPassFail(stringScanLarge(&index,25,GT,40,LT), 14)
#endif
// 	checkPassFail(stringScanLarge(&index,20,GTE,35,LTE), 16)
// 	checkPassFail(stringScanLarge(&index,-3,GT,3,LT), 3)
// 	checkPassFail(stringScanLarge(&index,996,GT,1001,LT), 4)
// 	checkPassFail(stringScanLarge(&index,0,GT,1,LT), 0)
// 	checkPassFail(stringScanLarge(&index,300,GT,400,LT), 99)
// 	checkPassFail(stringScanLarge(&index,3000,GTE,4000,LT), 1000)
	checkPassFail(stringScanLarge(&index,3000,GTE,400000,LT), 397000)
}


// -----------------------------------------------------------------------------
// stringTests
// -----------------------------------------------------------------------------

void stringTests()
{
  std::cout << "Create a B+ Tree index on the string field" << std::endl;
  BTreeIndex index(relationName, stringIndexName, bufMgr, offsetof(tuple,s), STRING);

	// run some tests
#ifdef DEBUGSTRING
    int out = stringScan(&index,25,GT,40,LT);
std::cout<<" stringScan(&index,25,GT,40,LT); RESULT is "<<out<<std::endl;
	checkPassFail(out, 14)
#else
	checkPassFail(stringScan(&index,25,GT,40,LT), 14)
#endif
	checkPassFail(stringScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(stringScan(&index,-3,GT,3,LT), 3)
	checkPassFail(stringScan(&index,225,GT,1001,LT), 1001-225-1)
	checkPassFail(stringScan(&index,226,GT,1001,LT), 1001-226-1)
	checkPassFail(stringScan(&index,227,GT,1001,LT), 1001-227-1)
	checkPassFail(stringScan(&index,228,GT,1001,LT), 1001-228-1)
	checkPassFail(stringScan(&index,229,GT,1001,LT), 1001-229-1)
	checkPassFail(stringScan(&index,225,GTE,1001,LT), 1001-225)
	checkPassFail(stringScan(&index,226,GTE,1001,LT), 1001-226)
	checkPassFail(stringScan(&index,227,GTE,1001,LT), 1001-227)
	checkPassFail(stringScan(&index,228,GTE,1001,LT), 1001-228)
	checkPassFail(stringScan(&index,229,GTE,1001,LT), 1001-229)
	checkPassFail(stringScan(&index,996,GT,1001,LT), 4)
	checkPassFail(stringScan(&index,0,GT,1,LT), 0)
	checkPassFail(stringScan(&index,300,GT,400,LT), 99)
	checkPassFail(stringScan(&index,3000,GTE,4000,LT), 1000)
}


int stringScanLarge(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
#endif

  char lowValStr[100];
  sprintf(lowValStr, "%010d string record",lowVal);
  char highValStr[100];
  sprintf(highValStr,"%010d string record",highVal);

  int numResults = 0;

#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
#endif
    try
    {
      index->startScan(lowValStr, lowOp, highValStr, highOp);
    }
    catch(NoSuchKeyFoundException e)
    {
      std::cout << "No Key Found satisfying the scan criteria." << std::endl;
      return 0;
    }


#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
#endif

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
  std::cout<<" scanRid.page_number is "<< scanRid.page_number<<std::endl;
#endif
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "rid:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
#ifdef DEBUGSTRING
  std::cout<<" catch IndexScanCompletedException !!!"<<std::endl;
#endif
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}


int stringScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
#endif
  char lowValStr[100];
  sprintf(lowValStr,"%05d string record",lowVal);
  char highValStr[100];
  sprintf(highValStr,"%05d string record",highVal);

  int numResults = 0;

#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
#endif
    try
    {
      index->startScan(lowValStr, lowOp, highValStr, highOp);
    }
    catch(NoSuchKeyFoundException e)
    {
      std::cout << "No Key Found satisfying the scan criteria." << std::endl;
      return 0;
    }


#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
#endif

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
#ifdef DEBUG
  std::cout<<" start stringScan.cpp:"<<__LINE__<<std::endl;
  std::cout<<" scanRid.page_number is "<< scanRid.page_number<<std::endl;
#endif
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "rid:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(IndexScanCompletedException e)
		{
#ifdef DEBUGSTRING
  std::cout<<" catch IndexScanCompletedException !!!"<<std::endl;
#endif
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

// -----------------------------------------------------------------------------
// errorTests
// -----------------------------------------------------------------------------

void errorTests()
{
	std::cout << "Error handling tests" << std::endl;
	std::cout << "--------------------" << std::endl;
	// Given error test

	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);
	
  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
	for(int i = 0; i <10; i++ ) 
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);

  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
	
	int int2 = 2;
	int int5 = 5;

	// Scan Tests
	std::cout << "Call endScan before startScan" << std::endl;
	try
	{
		index.endScan();
		std::cout << "ScanNotInitialized Test 1 Failed." << std::endl;
	}
	catch(ScanNotInitializedException e)
	{
		std::cout << "ScanNotInitialized Test 1 Passed." << std::endl;
	}
	
	std::cout << "Call scanNext before startScan" << std::endl;
	try
	{
		RecordId foo;
		index.scanNext(foo);
		std::cout << "ScanNotInitialized Test 2 Failed." << std::endl;
	}
	catch(ScanNotInitializedException e)
	{
		std::cout << "ScanNotInitialized Test 2 Passed." << std::endl;
	}
	
	std::cout << "Scan with bad lowOp" << std::endl;
	try
	{
  	index.startScan(&int2, LTE, &int5, LTE);
		std::cout << "BadOpcodesException Test 1 Failed." << std::endl;
	}
	catch(BadOpcodesException e)
	{
		std::cout << "BadOpcodesException Test 1 Passed." << std::endl;
	}
	
	std::cout << "Scan with bad highOp" << std::endl;
	try
	{
  	index.startScan(&int2, GTE, &int5, GTE);
		std::cout << "BadOpcodesException Test 2 Failed." << std::endl;
	}
	catch(BadOpcodesException e)
	{
		std::cout << "BadOpcodesException Test 2 Passed." << std::endl;
	}


	std::cout << "Scan with bad range" << std::endl;
	try
	{
  	index.startScan(&int5, GTE, &int2, LTE);
		std::cout << "BadScanrangeException Test 1 Failed." << std::endl;
	}
	catch(BadScanrangeException e)
	{
		std::cout << "BadScanrangeException Test 1 Passed." << std::endl;
	}

	deleteRelation();
}

void deleteRelation()
{
	if(file1) {
		bufMgr->flushFile(file1);
		delete file1;
		file1 = NULL;
	} try {
		File::remove(relationName);
	} catch(FileNotFoundException e) {
	}
}




void createRelationForwardDup(int size)
{
  std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
  try {
    File::remove(relationName);
  } catch(FileNotFoundException e) {
  }

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
  PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < size; i++ ) {
    for ( int j = 1; j < 3 ; ++ j) {
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

    while(1) {
      try {
        new_page.insertRecord(new_data);
        break;
      } catch(InsufficientSpaceException e) {
        file1->writePage(new_page_number, new_page);
        new_page = file1->allocatePage(new_page_number);
      }
    }
    }
  }

  file1->writePage(new_page_number, new_page);
}


// duplicate key
void test7() 
{
  // create duplaite key
    const int size = 5000;

	std::cout << std::endl;
	std::cout << std::endl;
	std::cout << "----------------------" << std::endl;
	std::cout << "- test duplicate key -" << std::endl;
	std::cout << "----------------------" << std::endl;
	std::cout << std::endl;
	std::cout << std::endl;

	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
    createRelationForwardDup(size);
// 	indexTests();


  {
    {
      std::cout << "Create a B+ Tree index on the integer field" << std::endl;
      BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

        // run some tests
        checkPassFail(intScan(&index,25,GT,40,LT), 28) // 14 
        checkPassFail(intScan(&index,20,GTE,35,LTE), 32)  // 16 
        checkPassFail(intScan(&index,-3,GT,3,LT), 6)  //6 
        checkPassFail(intScan(&index,338,GT,1001,LT),  2*(1001-338-1))
        checkPassFail(intScan(&index,339,GT,1001,LT),  2*(1001-339-1))
        checkPassFail(intScan(&index,340,GT,1001,LT),  2*(1001-340-1))
        checkPassFail(intScan(&index,341,GT,1001,LT),  2*(1001-341-1))
        checkPassFail(intScan(&index,342,GT,1001,LT),  2*(1001-342-1))
        checkPassFail(intScan(&index,338,GTE,1001,LT), 2*(1001-338))
        checkPassFail(intScan(&index,339,GTE,1001,LT), 2*(1001-339))
        checkPassFail(intScan(&index,340,GTE,1001,LT), 2*(1001-340))
        checkPassFail(intScan(&index,341,GTE,1001,LT), 2*(1001-341))
        checkPassFail(intScan(&index,342,GTE,1001,LT), 2*(1001-342))
        checkPassFail(intScan(&index,996,GT,1001,LT), 8) // 4
        checkPassFail(intScan(&index,0,GT,1,LT), 0)  // 0 
        checkPassFail(intScan(&index,300,GT,400,LT), 198)  // 99
        checkPassFail(intScan(&index,3000,GTE,4000,LT), 2000) // 1000
    }
	try {
		File::remove(intIndexName);
	} catch(FileNotFoundException e) {
  	}
  }
	deleteRelation();

}

// delete an entry
void test8()
{
    const int size = 5000; 
	std::cout << "\n\n------------------------------------\n";
	std::cout <<     "- test delete entry without adjust -\n";
	std::cout <<     "------------------------------------\n\n\n";
    // create normal relation
    createRelationForward(size);
    { // create index, delete, and check
      {
        std::cout << "Create a B+ Tree index on the integer field" << std::endl;
        BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
        checkPassFail(intScan(&index,4995,GTE,5000,LT), 5)
        // test 1. delete keys without adjuct tree
        for (int i = 1 ; i < 5 ; ++i ) {
          int key = size - i ;
          index.deleteEntry((void*)(&key));
          std::cout<<"delete "<<key<<"\n";
          checkPassFail(intScan(&index,4000,GTE, 5000,LT), 1000-i)
        }
#ifdef DEBUGPRINTTREE
        index.printTree();
#endif
      }
      try { // remove index file
        File::remove(intIndexName);
      } catch(FileNotFoundException e) {
      }
    }
    deleteRelation();
}


// delete an entry leads to redistribution
void test9()
{
    const int size = 5000; 
	std::cout << "\n\n------------------------------------------\n";
	std::cout <<     "- test delete entry until redistribution -\n";
	std::cout <<     "------------------------------------------\n\n\n";

    // create normal relation
    createRelationForward(size);
    
    {
      // create index, delete, and check
      {
        std::cout << "Create a B+ Tree index on the integer field" << std::endl;
        BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

        checkPassFail(intScan(&index,4000,GTE,4500,LT), 500)
        // test 2. delete key that needs redistribution
        for (int i = 1 ; i < 5 ; ++i ) {
          int key = 4300 - i ;
          std::cout<<"delete "<<key<<"\n";
          index.deleteEntry((void*)(&key));
          checkPassFail(intScan(&index,4000,GTE,4500,LT), 500-i)
        }
#ifdef DEBUGPRINTTREE
        index.printTree();
#endif
      }
      try { // remove index file
        File::remove(intIndexName);
      } catch(FileNotFoundException e) {}
    }
    deleteRelation();
}



// delete an entry leads merge leaf
void test10()
{
    const int size = 5000; 
	std::cout << "\n\n--------------------------------------\n";
	std::cout <<     "- test delete entry until merge leaf -\n";
	std::cout <<     "--------------------------------------\n\n\n";
    // create normal relation
    createRelationForward(size);
    
    { // create index, delete, and check
      {
        std::cout << "Create a B+ Tree index on the integer field" << std::endl;
        BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
        // delete the last couple of keys
        int key = 0;
        // test 3. delete key that needs merge 
	    checkPassFail(intScan(&index,25,GT,40,LT), 14)
        int end = 30;
        for ( int i = 27 ; i <= end ; ++i ) {
          key = i;
          index.deleteEntry((void*)(&key));
	      checkPassFail(intScan(&index,25,GT,40,LT), 14+26-i)
        }
#ifdef DEBUGPRINTTREE
        index.printTree();
#endif
      }
      // remove index file
      try {
        File::remove(intIndexName);
      } catch(FileNotFoundException e) {
      }
    }
    deleteRelation();
}



//delete until merge non leaf

void test11()
{
    const int size = 5000; 
	std::cout << "\n\n------------------------------------------\n";
	std::cout <<     "- test delete entry until merge non leaf -\n";
	std::cout <<     "------------------------------------------\n\n\n";
    // create normal relation
    createRelationForward(size);
    
    { // create index, delete, and check
      {
        std::cout << "Create a B+ Tree index on the integer field" << std::endl;
        BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
        // delete the last couple of keys
        int key = 0;
        // test 4. delete key that needs merge non leaf
	    checkPassFail(intScan(&index,25,GT,40,LT), 14)
        int end = 4800;
        for ( int i = 1 ; i <= end ; ++i ) {
          key = i;
          index.deleteEntry((void*)(&key));
//  	      checkPassFail(intScan(&index,0,GT,5000,LT), 4999-i)
        }
 	      checkPassFail(intScan(&index,0,GT,5000,LT), 4999-4800)
#ifdef DEBUGPRINTTREE
        index.printTree();
#endif
      }
      // remove index file
      try {
        File::remove(intIndexName);
      } catch(FileNotFoundException e) {
      }
    }
    deleteRelation();
}


// delete from empty tree

void test12()
{
    const int size = 1; 
	std::cout << "\n\n-------------------------------------\n";
	std::cout <<     "- test delete entry from empty tree -\n";
	std::cout <<     "-------------------------------------\n\n\n";
    // create normal relation
    createRelationForward(size);
    
    { // create index, delete, and check
      {
        try {
          std::cout << "Create a B+ Tree index on the integer field" << std::endl;
          BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
          int key = 0;
          index.printTree();
          index.deleteEntry((void*)(&key));
          index.printTree();
          // test 5. delete key from empty tree
          index.deleteEntry((void*)(&key));
          index.printTree();
        } catch (EmptyBTreeException e) {
          std::cout<<" empty tree test passed\n";
        }
      }
      // remove index file
      try {
        File::remove(intIndexName);
      } catch(FileNotFoundException e) {
      }
    }
    deleteRelation();
}

