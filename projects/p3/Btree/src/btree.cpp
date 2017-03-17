
// #define DEBUG
// #define DEBUGle // catch invalid record exception
// #define DEBUGLEAF
// #define DEBUGNONLEAF
// #define DEBUGFINDLEAF
// #define DEBUGPRINTTREE
// #define ADDZERO

// #define DEBUGMORE
// #define DEBUGSCAN
// #define DEBUGCOMPARE
// #define DEBUGSTRING
// #define DEBUGCOPY



/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

// #include "exceptions/file_exists_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/invalid_record_exception.h"

namespace badgerdb
{


template<class T>
const int compare(const T a, const T b)
{
#ifdef DEBUGCOMPARE
  std::cout<<"Generic compare called"<<std::endl;
#endif
  return a - b;
}

template<> // explicit specialization for T = void
const int compare<char[STRINGSIZE]>( char a[STRINGSIZE], char b[STRINGSIZE])
{
  return strncmp(a,b,STRINGSIZE);



template <class T>
const void copyKey( T &a, T &b )
{
  a = b;
}


template<> // explicit specialization for T = void
const void copyKey<char[STRINGSIZE]>( char (&a)[STRINGSIZE], char (&b)[STRINGSIZE])
{
  strncpy(a,b,STRINGSIZE);
}


// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
      : bufMgr(bufMgrIn),               // initialized data field
        attributeType(attrType),
        attrByteOffset(attrByteOffset)
{
  {// outIndexName is the name of the index file
   std::ostringstream idxStr;
   idxStr << relationName << '.' << attrByteOffset;
   outIndexName = idxStr.str(); 
  } // idxStr goes out of scope here, so idxStr is automatically closed.

#ifdef DEBUG
  std::cout<<"Create a new BTreeIndex object"<<std::endl;
#endif

  // 1. set up the BTreeIndex data field
  switch  (attributeType ) {
    case INTEGER:
      leafOccupancy = INTARRAYLEAFSIZE;
      nodeOccupancy = INTARRAYNONLEAFSIZE;
      break;
    case DOUBLE:
      leafOccupancy = DOUBLEARRAYLEAFSIZE;
      nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
      break;
    case STRING:
      leafOccupancy = STRINGARRAYLEAFSIZE;
      nodeOccupancy = STRINGARRAYNONLEAFSIZE;
      break;
    default:
      std::cout<<"Unsupported data type\n";
  }

#ifdef DEBUG
    std::cout<<"Leaf Size: " << leafOccupancy<< std::endl;
    std::cout<<"NonLeaf Size: " << nodeOccupancy<< std::endl;
#endif

    scanExecuting = false;
    nextEntry = -1;
    currentPageNum = 0;


  // 2. check if the index file exits. open or create new
  //   if it is new, create metanode, create the root node
  try { // try read old file
    file = new BlobFile(outIndexName,false);
#ifdef DEBUG
std::cout<<"Reading old index file!!!\n";
#endif
    Page *tempPage;
    headerPageNum = file->getFirstPageNo();
    bufMgr->readPage(file, headerPageNum, tempPage);
//     IndexMetaInfo *metaInfo = (IndexMetaInfo*) tempPage;
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(tempPage);
    rootPageNum = metaInfo->rootPageNo;
#ifdef DEBUG
  std::cout<<"<>headerPageNum: "<<headerPageNum<<std::endl;
  std::cout<<"<>relationName: "<<relationName<<std::endl;
  std::cout<<"  metaInfo->relationName: "<<(*metaInfo).relationName<<std::endl;
  std::cout<<"<>attrByteOffset: "<<attrByteOffset<<std::endl;
  std::cout<<"  metaInfo->attrByteOffset: "<<(*metaInfo).attrByteOffset<<std::endl;
  std::cout<<"<>attrType: "<<attrType<<std::endl;
  std::cout<<"  metaInfo->attrType: "<<metaInfo->attrType<<std::endl;
  std::cout<<"<>rootPageNum: "<<rootPageNum<<std::endl;
  std::cout<<"  metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif

    bufMgr->unPinPage(file, headerPageNum, false);
    if ( relationName.compare(metaInfo->relationName) != 0
        || metaInfo->attrByteOffset != attrByteOffset
        || metaInfo->attrType != attrType
       ) {
      // meta info does not match in index file, clear and return;
//       delete file;
      std::cout<<"Meta info does not match the index!\n";
      delete file;
      file = NULL;
      return;
    }
  } catch (FileNotFoundException e ) {
//     delete file;
    // This is a new index file, create and contruct the new index file
#ifdef DEBUG
std::cout<<"Creating new index file!!!\n";
#endif
    Page *tempPage;
    file = new BlobFile(outIndexName, true);

    bufMgr->allocPage(file, headerPageNum, tempPage);
//     IndexMetaInfo *metaInfo = (IndexMetaInfo *) tempPage;
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(tempPage);
    bufMgr->allocPage(file, rootPageNum, tempPage);

    // assign index meta info
    std::copy(relationName.begin(), relationName.end(), metaInfo->relationName);
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;
#ifdef DEBUG
  std::cout<<"  rootPageNum: "<<rootPageNum<<std::endl;
  std::cout<<"<>metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif

    // construct root page
    if ( attributeType == INTEGER ) {
      LeafNodeInt* intRootPage = reinterpret_cast<LeafNodeInt*>(tempPage);
      intRootPage->size = 0;
      intRootPage->parentPageNo = 0;
      intRootPage->rightSibPageNo = 0;
    } else if ( attributeType == DOUBLE ) {
      LeafNodeDouble* doubleRootPage = reinterpret_cast<LeafNodeDouble*>(tempPage);
      doubleRootPage->size = 0;
      doubleRootPage->parentPageNo = 0;
      doubleRootPage->rightSibPageNo = 0;
    } else if ( attributeType == STRING ) {
      LeafNodeString* stringRootPage = reinterpret_cast<LeafNodeString*>(tempPage);
      stringRootPage->size = 0;
      stringRootPage->parentPageNo = 0;
      stringRootPage->rightSibPageNo = 0;
    } else {
      std::cout<<"Unsupported data type\n";
    }
    // done with meta page and root page
    bufMgr->unPinPage(file, rootPageNum, true);
    bufMgr->unPinPage(file, headerPageNum, true);


#ifdef DEBUG
  std::cout<<"<>metaInfo->relationName: "<<metaInfo->relationName<<std::endl;
  std::cout<<"<>metaInfo->attrByteOffset: "<<metaInfo->attrByteOffset<<std::endl;
  std::cout<<"<>metaInfo->attrType: "<<metaInfo->attrType<<std::endl;
  std::cout<<"<>metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif
#ifdef DEBUG
  std::cout<<"headerPageNum: "<<headerPageNum<<std::endl;
  std::cout<<"rootPageNum: "<<rootPageNum<<std::endl;
#endif

    // build B-Tree: insert the record into the B-Tree
    buildBTree(relationName);

  }


#ifdef DEBUG
    std::cout<<"File name of index: "<<file->filename()<<std::endl;
    std::cout<<"Leaf Size: " << leafOccupancy<< std::endl;
    std::cout<<"NonLeaf Size: " << nodeOccupancy<< std::endl;
#endif


#ifdef DEBUGPRINTTREE

    if ( attributeType == INTEGER ) {
      printTree<int, struct NonLeafNodeInt, struct LeafNodeInt>();
    }
    else if ( attributeType == DOUBLE ) {
      printTree<double, struct NonLeafNodeDouble, struct LeafNodeDouble>();
    }
    else if ( attributeType == STRING ) {
      printTree<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>();
    } else {
      std::cout<<"Unsupported data type\n";
    }
#endif



}


// -----------------------------------------------------------------------------
// BTreeIndex::buildBTree -- build the initial tree from relation file
// -----------------------------------------------------------------------------

const void BTreeIndex::buildBTree(const std::string & relationName)
{
    FileScan fscan(relationName, bufMgr);
    try {
      RecordId scanRid;
      while(1)
      {
        fscan.scanNext(scanRid);
        //Assuming RECORD.i is our key, lets extract the key, which we know is
        //INTEGER and whose byte offset is also know inside the record. 
        std::string recordStr = fscan.getRecord();
        const char *record = recordStr.c_str();
        void *key = (void *)(record + attrByteOffset);
        insertEntry(key, scanRid);
#ifdef DEBUGMORE
    if ( attributeType == INTEGER ) {
      std::cout << "Extracted : " << *((int*)key) << std::endl;
    }
    else if ( attributeType == DOUBLE ) {
      std::cout << "Extracted : " << *((double*)key) << std::endl;
    }
    else if ( attributeType == STRING ) {
      std::cout << "Extracted : " << ((char*)key) << std::endl;
    } else {
      std::cout<<"Unsupported data type\n";
    }
#endif
      }
    } catch(EndOfFileException e) {
#ifdef DEBUG
      std::cout << "Read all records" << std::endl;
      std::cout << "BTree initialized" << std::endl;
#endif
    } 
#ifdef DEBUGle
    catch ( InvalidRecordException e) {
    }
#endif

}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    //  clean up any state varibles ,
    //  unpinning pages
    //  flush the index file
    //  delete the index file
  
    // delete the rkpair in the BTree  


#ifdef DEBUG
  std::cout<<"BTreeIndex destructor"<<std::endl;
#endif

    if ( currentPageNum ) {
      try {
        bufMgr->unPinPage(file, currentPageNum, false);
      } catch ( PageNotPinnedException e) {
#ifdef DEBUG
        std::cout<<"currentPageNum is not pinned"<<std::endl;
      } catch ( BadgerDbException e ) {
        std::cout<<"other exception"<<std::endl;
        throw e;
#endif
      }
    }

    scanExecuting = false;
#ifdef DEBUG
  std::cout<<" try to delete file object "<<std::endl;
#endif
    if ( file ) {
#ifdef DEBUG
  std::cout<<" delete file object "<<std::endl;
#endif
      // unpin
      bufMgr->flushFile(file);
      delete file;
      file = NULL;
    }

#ifdef DEBUG
    std::cout<<"BTreeIndex destructor called"<<std::endl;
#endif
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // determine the type, find the leaf node to insert, and insert
    if ( attributeType == INTEGER ) {
        RIDKeyPair<int> rkpair;
        int intKey = *(int*)key;
        rkpair.set(rid, intKey);
#ifdef DEBUGMORE
  std::cout<< " call findLeafNode from insertEntry " << std::endl;
#endif
        PageId leafToInsert = 
          findLeafNode<int, struct NonLeafNodeInt>(rootPageNum, intKey);
        insertLeafNode<int, struct NonLeafNodeInt, struct LeafNodeInt>(leafToInsert, rkpair);
    } else if ( attributeType == DOUBLE ) {
        RIDKeyPair<double> rkpair;
        double doubleKey = *(double*)key;
        rkpair.set(rid, doubleKey);
        PageId leafToInsert =
          findLeafNode<double, struct NonLeafNodeDouble>(rootPageNum,doubleKey);
        insertLeafNode<double, struct NonLeafNodeDouble, struct LeafNodeDouble>(leafToInsert, rkpair);
    } else if ( attributeType == STRING ) {
        RIDKeyPair<char[STRINGSIZE]> rkpair;
        strncpy(rkpair.key, (char*)key, STRINGSIZE);
#ifdef ADDZERO
// for the purpose to print tree, need to terminate the string by '\0'
 rkpair.key[STRINGSIZE-1] ='\0';
#endif
#ifdef DEBUG
std::cout<< " when inserting, rid.page_number is "<< rid.page_number<<std::endl;
#endif
        rkpair.rid = rid;
        PageId leafToInsert = 
          findLeafNode<char[STRINGSIZE], struct NonLeafNodeString>(rootPageNum,rkpair.key);
        insertLeafNode<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>(leafToInsert, rkpair);
    } else {
        std::cout<<"Unsupported data type\n";
    }

}


// -----------------------------------------------------------------------------
// BTreeIndex::findLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode>
const PageId BTreeIndex::findLeafNode(PageId pageNo, T &key)
{
#ifdef DEBUGMORE
  std::cout << "in findLeafNode rootPageNum is "<< rootPageNum << std::endl;
#endif
  if ( rootPageNum == 2 ) { // root is leaf
#ifdef DEBUG
  std::cout << " ROOT IS LEAF!!!!!!" << std::endl;
#endif
    return rootPageNum; 
  }

  // pageNo should points to a non-leaf node page
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  
  int size = thisPage->size;
#ifdef DEBUGMORE
  std::cout << "=-=-=-= size of thisPage is " << size << std::endl;
#endif

//   //TODO Use binary search to speed up

  int index = 0;
#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
#ifdef DEBUGFINDLEAF
  std::cout<< "  findLeafNode: search for index "<<index;
  std::cout<< " , key is " << key;
  std::cout<<", thisPage->keyArray[index] is "<<thisPage->keyArray[index];
  std::cout<<", thisPage->pageNoArray[index] is "<<thisPage->pageNoArray[index];
  std::cout<< std::endl;
#endif
  while ( index < size && compare<T>(key, thisPage->keyArray[index]) > 0 )
  {
#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
    ++index;
#ifdef DEBUGFINDLEAF
    if (index < size ) {
      std::cout<< "  findLeafNode: search for index "<<index;
      std::cout<< " , key is " << key;
      std::cout<<", thisPage->keyArray[index] is "<<thisPage->keyArray[index];
  std::cout<<", thisPage->pageNoArray[index] is "<<thisPage->pageNoArray[index];
      std::cout<< std::endl;
    } else {
      std::cout<<"  findLeafNode: index == size, break;";
      std::cout<< std::endl;
    }
#endif
  }

#ifdef DEBUGFINDLEAF
  std::cout<< "  index in this non-leaf node to look at/insert is " << index << std::endl;
#endif

  PageId leafNodeNo = thisPage->pageNoArray[index];
  int thisPageLevel  = thisPage->level;
  bufMgr->unPinPage(file, pageNo, false);
  if ( thisPageLevel == 0 ) { // next level is non-leaf node
#ifdef DEBUGFINDLEAF
  std::cout<< "  findLeafNode: next level is still non-leaf, continue search on pageNo " << leafNodeNo/*nextPageNo*/ << std::endl;
#endif
    leafNodeNo = findLeafNode<T, T_NonLeafNode>(leafNodeNo, key);
  }
#ifdef DEBUGFINDLEAF
  else {
    std::cout<< "  findLeafNode: next level is leaf, this page is " << pageNo<<std::endl;
    std::cout<< "  findLeafNode: next level is leaf, its PageId is " << leafNodeNo<<std::endl;
  }
#endif

#ifdef DEBUGMORE
// #ifdef DEBUG
  std::cout << " Leaf find by findLeafNode to insert is "<< leafNodeNo << std::endl;
  if ( leafNodeNo == 0 ) exit(0);
#endif

  return leafNodeNo;

}



// -----------------------------------------------------------------------------
// BTreeIndex::insertLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode> 
const void BTreeIndex::insertLeafNode(PageId pageNo, RIDKeyPair<T> rkpair)
{
  // root and leaf do not have to be at least half full

  Page *tempPage; // temperary point, can be reused after cast
  bufMgr->readPage(file, pageNo, tempPage);
  T_LeafNode * thisPage = reinterpret_cast<T_LeafNode*>(tempPage);
  T thisKey;
//   copyKey((void*)(&thisKey),(void *)(&(rkpair.key)));
  copyKey((thisKey), ((rkpair.key)));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of thisKey is ";
  std::cout<<thisKey<<" and it should be "<<rkpair.key<<std::endl;
#endif

  int size = thisPage->size;
// #ifdef DEBUGMORE
#ifdef DEBUGMORE
  std::cout<<" Inserting to pageNo " << pageNo <<std::endl;
  std::cout<<"before insert into leaf, size of the page is "<<size<<std::endl;
#endif
  if ( size < leafOccupancy ) {
#ifdef DEBUGMORE
  std::cout<<"size is smaller than "<<leafOccupancy<<", just insert"<<std::endl;
#endif
    // Still have space, just insert into correct position
    int index = 0;
    // find the index that this record should be
    // TODO Use binary search
//     while ( index <size && thisKey > thisPage->keyArray[index] ) ++index;
#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
    while ( index <size && compare<T>(thisKey, thisPage->keyArray[index]) > 0) {

#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
      ++index;
    }
#ifdef DEBUGMORE
  std::cout<<"Afte search, the index to insert is "<<index<<std::endl;
#endif

    // use memmove
    memmove((void*)(&(thisPage->keyArray[index+1])),
            (void*)(&(thisPage->keyArray[index])), sizeof(T)*(size-index));
    memmove((void*)(&(thisPage->ridArray[index+1])),
            (void*)(&(thisPage->ridArray[index])), sizeof(RecordId)*(size-index));

    // insert the current key
    copyKey(((thisPage->keyArray[index])), (thisKey));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of thisPage->keyArray[index] is ";
  std::cout<<thisPage->keyArray[index]<<" and it should be "<<thisKey<<std::endl;
#endif

    thisPage->ridArray[index] = rkpair.rid;

    // increase the size by 1;
    (thisPage->size)++;
    bufMgr->unPinPage(file, pageNo, true);
  } else {
#ifdef DEBUGMORE
  std::cout<<"size is larger than "<<leafOccupancy<<", need split"<<std::endl;
#endif
    // not enough space, need to split

    // determine where this rkpair should go
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
    bool insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;
#ifdef DEBUGMORE
  std::cout<<"insertLeftNode?? "<<insertLeftNode<<std::endl;
#endif

    // close the page to split
    bufMgr->unPinPage(file, pageNo, false);

    // split the node and get the newly allocated PageId
    PageId rightPageNo = splitLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNo);
#ifdef DEBUGMORE
  std::cout<<"rightPageNo is "<<rightPageNo<<std::endl;
#endif

    // insert to proper page
    PageId pageNoToInsert = pageNo;
    if ( !insertLeftNode ) // insert second page
      pageNoToInsert = rightPageNo;
    insertLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNoToInsert, rkpair);

  }
}


// // -----------------------------------------------------------------------------
// // BTreeIndex::copyKey
// // -----------------------------------------------------------------------------
// 
// // const void BTreeIndex::copyKey( Page * dest, int desIndex, 
// //                                 Page * scr, int scrIndex)
// 
// const void BTreeIndex::copyKey( void * dest, void * scr)
// {
//   if ( attributeType == INTEGER ) {
//     int *intDest = (int*)dest;
//     *intDest = *((int*)scr);
//   } else if ( attributeType == DOUBLE ) {
//     double *doubleDest = (double*)dest;
//     *doubleDest =  *((double*)scr);
//   } else if ( attributeType == STRING ) {
//     strncpy((char*)dest, (char*)scr, STRINGSIZE);
// #ifdef ADDZERO
//  char*destc = (char*)dest;
//  destc[STRINGSIZE-1] ='\0';
// #endif
//   }
// }
// further test:  use strncpy for all (  sizeof(int) )




// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::splitLeafNode(PageId pageNo)
{
    Page *tempPage; // can be reused
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file, firstPageNo, tempPage);
    T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
    PageId secondPageNo;
#ifdef DEBUGLEAF
  std::cout<<"secondPageNo before init is "<<secondPageNo<<std::endl;
#endif
    bufMgr->allocPage(file, secondPageNo, tempPage);
#ifdef DEBUGLEAF
  std::cout<<"secondPageNo after init is "<<secondPageNo<<std::endl;
#endif
    T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);


#ifdef DEBUGLEAF
  if ( secondPageNo > 1000 ) {
    std::cout<<" Second page not set properly"<<std::endl;
    exit(1);
  }
  std::cout<<std::endl;
  std::cout<<"   --------------------------- Split Leaf Node, this node No is "<< firstPageNo<<std::endl;
  std::cout<<"   ---------------------------  new node no is "<<secondPageNo<<std::endl;
  std::cout<<std::endl;


  std::cout<<" firstPage->parentPageNo is "<<firstPage->parentPageNo<<std::endl;
  std::cout<<std::endl;

#endif

    // set the parent properly
    secondPage->parentPageNo = firstPage->parentPageNo;

    // set the sibling properly
    secondPage->rightSibPageNo = firstPage->rightSibPageNo;
    firstPage->rightSibPageNo = secondPageNo;

    // middle one, as the new key in parent
    // firstPage has more keys if odd size
    // copy the second half keys to secondPage
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    // use memmove to split 
    memmove((void*)(&(secondPage->keyArray[0])),
            (void*)(&( firstPage->keyArray[midIndex])),
            sizeof(T)*(leafOccupancy-midIndex));
    memmove((void*)(&(secondPage->ridArray[0])),
            (void*)(&( firstPage->ridArray[midIndex])),
            sizeof(RecordId)*(leafOccupancy-midIndex));

    firstPage->size = midIndex;
    secondPage->size = leafOccupancy - midIndex;

    T copyUpKey;
//     copyKey((void*)(&copyUpKey), (void*)(&(firstPage->keyArray[midIndex])));
    copyKey((copyUpKey), ((firstPage->keyArray[midIndex])));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of copyUpKey is ";
  std::cout<<copyUpKey<<" and it should be "<<firstPage->keyArray[midIndex]<<std::endl;
#endif

//     bool rootIsLeaf = firstPage->parentPageNo < 1;
    bool rootIsLeaf = rootPageNum == 2;


    // check if here is parent
    //
    // I think, as long as the parent page no is not 0, it is not root 
    // but we can do extra check, TODO
    if ( !rootIsLeaf ) { // normal parent, insert new key and pageNo

      PageId firstPageParentNo = firstPage->parentPageNo;
      // done with first and second page
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);
#ifdef DEBUGLEAF
  std::cout << "in splitleafnode, new secondPageNo is "<< secondPageNo << std::endl;
#endif
      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (firstPageParentNo, copyUpKey,secondPageNo);
    } else {  // it is the root leaf node
      // allocate new parent node
      PageId parentPageNo;
      bufMgr->allocPage(file, parentPageNo, tempPage);
      T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

      // update in class field
      rootPageNum = parentPageNo;
      // update the meta info
      bufMgr->readPage(file, headerPageNum, tempPage);
      IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
      metaPage->rootPageNo = parentPageNo;
      bufMgr->unPinPage(file, headerPageNum, true);

      // update child pages' parent
      firstPage->parentPageNo = parentPageNo;
      secondPage->parentPageNo = parentPageNo;

      // done with first and second page
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);

      // construct the parent node
      parentPage->level = 1;  // just above leaf
      parentPage->size = 1;   // one key, points to first and second page
      copyKey(((parentPage->keyArray[0])), ((copyUpKey)));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of parentPage->keyArray[0] is ";
  std::cout<<parentPage->keyArray[0]<<" and it should be "<<copyUpKey<<std::endl;
#endif
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;

      // done with parentPage, save changes
      bufMgr->unPinPage(file, parentPageNo, true);
    }

    // return the second page number created
    return secondPageNo;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::insertNonLeafNode(PageId pageNo, T &key, PageId childPageNo)
{

    Page *tempPage;
    bufMgr->readPage(file, pageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    T thisKey;
    copyKey(thisKey, key);
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of thisKey is ";
  std::cout<<thisKey<<" and it should be "<<key<<std::endl;
#endif
    int size = thisPage->size;
#ifdef DEBUGNONLEAF
  std::cout<<"size " << size <<"  nodeOccupancy "<< nodeOccupancy<<std::endl;
#endif
    if ( size < nodeOccupancy ) {
      // still have space, just insert
#ifdef DEBUGNONLEAF
  std::cout<<" non-leaf "<<pageNo<<" has enough space, just insert"<<std::endl;
#endif
      // find the correct position to insert the key
      int index = 0;
      // can use binary search to speed up to logN
      while ( index < size && compare<T>(thisKey, thisPage->keyArray[index]) > 0 ) {
        ++index;
      }

      // use memmove to shift all keys and pageNos right by 1 position
      memmove((void*)(&(thisPage->keyArray[index+1])),
              (void*)(&(thisPage->keyArray[index])), sizeof(T)*(size-index));
      memmove((void*)(&(thisPage->pageNoArray[index+2])),
              (void*)(&(thisPage->pageNoArray[index+1])), sizeof(PageId)*(size-index));

      // insert the current key
      copyKey(thisPage->keyArray[index], thisKey);
      thisPage->pageNoArray[index+1] = childPageNo;
      (thisPage->size)++;
      // done, close the file
      bufMgr->unPinPage(file, pageNo, true);
    } else {
#ifdef DEBUGNONLEAF
  std::cout<<" no enough space for this non-leaf, do split"<<std::endl;
#endif
      // need to split this non-leaf node, potentially propagate up

      // you know where to insert before split
//       int midIndex = nodeOccupancy/2 + nodeOccupancy%2;
      int midIndex = nodeOccupancy/2;
//       if ( !insertLeftNode ) {
//         midIndex += nodeOccupancy%2;
//       }


//       midIndex--;

      bool insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;
// std::cout<<"insertLeftNode?? "<<insertLeftNode<<std::endl;

//// DEBUG
//       insertLeftNode = true;

      if ( nodeOccupancy%2 == 0 ) {
        midIndex--;
        insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;
      }

      // possible to insert in the mid, aka, push up further


#ifdef DEBUGNONLEAF
  if ( insertLeftNode ) {
    std::cout<<" nonleaf split, insert left node "<<std::endl;
  } else {
    std::cout<<" nonleaf split, insert right node "<<std::endl;
  }
#endif

      // close the file for split
      bufMgr->unPinPage(file, pageNo, false);

      // split the non-leaf node, potentially affect upwards to the root
      PageId rightPageNo = splitNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (pageNo, midIndex);

      // if insert right part after split 
      //  update the secondPage's parent to new
      if ( ! insertLeftNode ) {
        bufMgr->readPage(file, childPageNo, tempPage);
        if ( thisPage->level == 1 ) { // above leaf
          T_LeafNode* tempLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
#ifdef DEBUGNONLEAF
    std::cout<<" Change childPage's parent from "<<tempLeafPage->parentPageNo;
    std::cout<<" to "<<rightPageNo<<std::endl;
#endif
          tempLeafPage->parentPageNo = rightPageNo;
        } else {
          T_NonLeafNode* tempLeafPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
#ifdef DEBUGNONLEAF
    std::cout<<" Change childPage's parent from "<<tempLeafPage->parentPageNo;
    std::cout<<" to "<<rightPageNo<<std::endl;
#endif
          tempLeafPage->parentPageNo = rightPageNo;
        }
        bufMgr->unPinPage(file, childPageNo, true);
      }
#ifdef DEBUGNONLEAF
      else {
        bufMgr->readPage(file, childPageNo, tempPage);
        if ( thisPage->level == 1  ) {
          T_LeafNode* tempLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
          std::cout<<" leave childPage's parent unchanged as "<<tempLeafPage->parentPageNo;
        } else {
          T_NonLeafNode* tempLeafPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
          std::cout<<" leave childPage's parent unchanged as "<<tempLeafPage->parentPageNo;
        }
        std::cout<<std::endl;
        bufMgr->unPinPage(file, childPageNo, false);
      }
#endif

      // insert to the proper page
      PageId pageNoToInsert = pageNo;
      if ( !insertLeftNode ) // insert second page
        pageNoToInsert = rightPageNo;
      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (pageNoToInsert, key, childPageNo);
    }
    
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::splitNonLeafNode(PageId pageNo, int midIndex)
{
    Page *tempPage; // can be reused
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file,  firstPageNo, tempPage);
    T_NonLeafNode * firstPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    PageId secondPageNo;
    bufMgr->allocPage(file, secondPageNo, tempPage);
    T_NonLeafNode* secondPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

#ifdef DEBUGNONLEAF
  std::cout<<std::endl;
  std::cout<<"   -----NON LEAF SPLIT-------- Split NON Leaf Node, this node No is "<< firstPageNo<<std::endl;
  std::cout<<"   ---------------------------  new node no is "<<secondPageNo<<std::endl;
  std::cout<<std::endl;
#endif

    // set the parent
    secondPage->parentPageNo = firstPage->parentPageNo;
    // set the level
    secondPage->level = firstPage->level;

//     // middle index will be push to parent
//     int midIndex = nodeOccupancy/2;
// //     if ( !insertLeftNode ) {
// //       midIndex += nodeOccupancy%2;
// //     }
//     if ( nodeOccupancy%2 == 0 ) {
//       if ( insertLeftNode ) {
//         midIndex--;
//       }
//     }


    // use memmove to split keys and pageNo s
    memmove((void*)(&(secondPage->keyArray[0])),
            (void*)(&( firstPage->keyArray[midIndex+1])),
            sizeof(T)*(nodeOccupancy-midIndex-1));
    memmove((void*)(&(secondPage->pageNoArray[0])),
            (void*)(&( firstPage->pageNoArray[midIndex+1])),
            sizeof(PageId)*(nodeOccupancy-midIndex));

    firstPage->size = midIndex;
    secondPage->size = nodeOccupancy - midIndex -1; // -1 bcz mid is pushed up

    // update splited pages' parentPageNo
#ifdef DEBUGNONLEAF
  std::cout<<" update page's parent no "<<std::endl;
  std::cout<<"midIndex is "<<midIndex<<std::endl;
  std::cout<<"secondPage->size is "<<secondPage->size<<std::endl;
#endif
    for ( int i = 0 ; i <= secondPage->size ; ++i ) {
      bufMgr->readPage(file, secondPage->pageNoArray[i], tempPage);
      T_LeafNode * tempLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
#ifdef DEBUGNONLEAF
  std::cout<<" update page "<<secondPage->pageNoArray[i];
  std::cout<<"'s parentPageNo from "<<tempLeafPage->parentPageNo;
  std::cout<<" to "<<secondPageNo<<std::endl;
#endif
      tempLeafPage->parentPageNo = secondPageNo;
      bufMgr->unPinPage(file, secondPage->pageNoArray[i], true);
    }
#ifdef DEBUGNONLEAF
  std::cout<<" CHECK that the update is really done"<<std::endl;
  for ( int i = 0 ; i <= secondPage->size ; ++i ) {
    bufMgr->readPage(file, secondPage->pageNoArray[i], tempPage);
    T_LeafNode * tempLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
    std::cout<<" parentPageNo is "<<tempLeafPage->parentPageNo;
    bufMgr->unPinPage(file, secondPage->pageNoArray[i], false);
  }
#endif


    T pushUpKey;
    copyKey(pushUpKey, firstPage->keyArray[midIndex]);
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of pushUpKey is ";
  std::cout<<pushUpKey<<" and it should be "<<firstPage->keyArray[midIndex]<<std::endl;
#endif

    bool currentNodeIsRoot = rootPageNum == firstPageNo;

#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of pushUpKey is ";
#endif

    if ( ! currentNodeIsRoot ) { // not root 
      // done with this two pages
      PageId parentPageNo = firstPage->parentPageNo;
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);
      // insert the pushUpKey to parent
      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (parentPageNo, pushUpKey, secondPageNo);
    } else { // this is the root to be splited, create new root
      PageId parentPageNo;
      bufMgr->allocPage(file, parentPageNo, tempPage);
      T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

      // update class fields
      rootPageNum = parentPageNo;
      // update meta info
      bufMgr->readPage(file, headerPageNum, tempPage);
      IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
      metaPage->rootPageNo = parentPageNo;
      bufMgr->unPinPage(file, headerPageNum, true);

      // update child pages' parent
      firstPage->parentPageNo = parentPageNo;
      secondPage->parentPageNo = parentPageNo;
      // done with this two pages
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);

      // construct the parent page
      parentPage->level = 0;
      parentPage->size = 1;
      copyKey(((parentPage->keyArray[0])),((pushUpKey)));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of parentPage->keyArray[0] is ";
  std::cout<<parentPage->keyArray[0]<<" and it should be "<<pushUpKey<<std::endl;
#endif
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;
      
      // done with parentPage, save changes
      bufMgr->unPinPage(file, parentPageNo, true);
      
    }

#ifdef DEBUGNONLEAF
  std::cout<<"\nRoot is "<<rootPageNum<<std::endl;
#endif

    // return newly alloc secondPage number
    return secondPageNo;

}

template <class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::printTree() {

std::cout<<"<><><><><><>Printing Tree " << std::endl;
  Page *tempPage;
  PageId currNo = rootPageNum;

  bool rootIsLeaf = rootPageNum == 2;
  bufMgr->readPage(file, currNo, tempPage);

  int lineSize = 20;

  try {

  if ( rootIsLeaf ) {
    // print the root
    T_LeafNode *currPage = reinterpret_cast<T_LeafNode*>(tempPage);
    int size = currPage->size;
std::cout<<" Root is leaf and the size is "<<size<<std::endl;
std::cout<<std::endl<<" PageId: "<<currNo<<std::endl;
    for ( int i = 0 ; i < size ; ++i) {
      if ( i%lineSize == 0 ) std::cout<<std::endl<<i<<": ";
      std::cout<<currPage->keyArray[i]<<" ";
    }
    std::cout<<std::endl<<" Root Leaf BTree printed"<<std::endl;
    bufMgr->unPinPage(file, currNo, false);
  } else {
    T_NonLeafNode *currPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    // travease down to the 1 level above leaf
    while ( currPage->level != 1 ) {
      PageId nextPageNo = currPage->pageNoArray[0];
      bufMgr->unPinPage(file, currNo, false);
      currNo = nextPageNo;
      bufMgr->readPage(file, currNo, tempPage);
      currPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
#ifdef DEBUG
  std::cout<< "currPage->level : "<<currPage->level<<std::endl;
#endif 
    }

    PageId leafNo = currPage->pageNoArray[0];
    bufMgr->unPinPage(file, currNo, false);

    // print the tree
    currNo = leafNo;
    bufMgr->readPage(file, currNo, tempPage);
    T_LeafNode *currLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
    while ( 1 ) {
      int size = currLeafPage->size;

std::cout<<std::endl<<" PageId: "<<currNo<<std::endl;
      for ( int i = 0 ; i < size ; ++i) {
        if ( i%lineSize == 0 ) std::cout<<std::endl<<i<<": ";
        std::cout<<currLeafPage->keyArray[i]<<" ";
      }
      std::cout<<std::endl;
      bufMgr->unPinPage(file, currNo, false);
#ifdef DEBUG
  std::cout<<" next sibling page number is "<<currLeafPage->rightSibPageNo<<std::endl;
#endif
      if ( currLeafPage->rightSibPageNo == 0 ) break;
      currNo = currLeafPage->rightSibPageNo;
      bufMgr->readPage(file, currNo, tempPage);
      currLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
    }
    std::cout<<std::endl<<" BTree printed"<<std::endl;
    bufMgr->unPinPage(file, currNo, false);
  }

  } catch ( PageNotPinnedException e ) {
  }
  

}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void*    lowValParm,
                                 const Operator lowOpParm,
                                 const void*    highValParm,
                                 const Operator highOpParm)
{

    // input validation
    if ( (lowOpParm  != GT && lowOpParm  != GTE) 
      || (highOpParm != LT && highOpParm != LTE) ) {
      throw BadOpcodesException();
    }
    if ( scanExecuting == true ) {// error, only one scan at a time
      std::cout<< "Scan Already started\n";
    } else {
      scanExecuting = true;
    }
   

    lowOp = lowOpParm;
    highOp = highOpParm;

    // deal with different types
    if ( attributeType == INTEGER ) {
      // store the low and high val
      lowValInt = *((int*)lowValParm);
      highValInt = *((int*)highValParm);
      startScanHelper<int, struct NonLeafNodeInt, struct LeafNodeInt>
          (lowValInt, highValInt);
    } else if ( attributeType == DOUBLE ) {
      lowValDouble = *((double*)lowValParm);
      highValDouble = *((double*)highValParm);
      startScanHelper<double, struct NonLeafNodeDouble, struct LeafNodeDouble>
          (lowValDouble, highValDouble);
    } else if ( attributeType == STRING ) {
//       char lowStringKey[STRINGSIZE];
//       char highStringKey[STRINGSIZE];
      strncpy(lowStringKey, (char*)lowValParm, STRINGSIZE);
      strncpy(highStringKey, (char*)highValParm, STRINGSIZE);
      

#ifdef ADDZERO
// for the purpose to print tree, need to terminate the string by '\0'
 lowStringKey[STRINGSIZE-1] ='\0';
 highStringKey[STRINGSIZE-1] ='\0';
#endif
      startScanHelper<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>
          (lowStringKey, highStringKey);

    } else {
       std::cout<<"Unsupported data type\n";
       exit(1);  // dangerous exit, need to clean the memory ??
    }

}


// -----------------------------------------------------------------------------
// BTreeIndex::startScanHelper
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::startScanHelper(T &lowVal, T &highVal)
{
      if ( lowVal > highVal ) {
        scanExecuting = false;
        throw BadScanrangeException();
      }
      
      // find the first page
      currentPageNum = findLeafNode<T, T_NonLeafNode>(rootPageNum, lowVal);
      // find the first index
      T_LeafNode*  thisPage;
#ifdef DEBUGSCAN
  std::cout<<" in startScanHelper, currentPageNum is "<<currentPageNum<<std::endl;
#endif

      bufMgr->readPage(file, currentPageNum, currentPageData);
      thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      
      int size = thisPage->size;
#ifdef DEBUGSCAN
  std::cout<<" in startScanHelper, size of this page "<<size<<std::endl;
#endif


#ifdef DEBUGSCAN
  std::cout<<"lowVal: |" << lowVal<<"||| thisPage->keyArray[size-1]: |";
  std::cout<<(thisPage->keyArray[size-1])<<"|"<< std::endl;
#endif
      // if lowValInt is larger than the last entry, it should goto next page
      // if it is equal, check the lowOPs to decide
      if ( compare<T>(lowVal , thisPage->keyArray[size-1]) > 0 
       ||( compare<T>(lowVal , thisPage->keyArray[size-1]) == 0 && lowOp == GT )) {
#ifdef DEBUGSCAN
  std::cout<<" in startScanHelper,goto next page "<<std::endl;
#endif
        PageId nextPageNo = thisPage->rightSibPageNo; 
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = nextPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      }

      // TODO Use binary search to get log(N) time complexity
      nextEntry = 0;
      while ( nextEntry < size && compare<T>(lowVal, thisPage->keyArray[nextEntry]) > 0 ) {
#ifdef DEBUGSCAN
  std::cout<<"lowVal: |" << lowVal<<"||| thisPage->keyArray[nextEntry]: |";
  std::cout<<(thisPage->keyArray[nextEntry])<<"|"<< std::endl;
#endif
        ++nextEntry;
      }

#ifdef DEBUGSCAN
  std::cout<<" in startScanHelper, currentPageNum is "<<currentPageNum<<std::endl;
  std::cout<<" in startScanHelper, nextEntry is "<<nextEntry<<std::endl;
#endif

#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
      if ( compare<T>(lowVal, thisPage->keyArray[nextEntry])==0 && lowOp == GT ) {
        // shiftToNextEntry TODO
        if ( ++nextEntry > thisPage->size ) { // move to next page
          if ( thisPage->rightSibPageNo == 0 ) { // no more pages
#ifdef DEBUGSTRING
  std::cout<<" IndexScanCompletedException throwed there at "<<__LINE__<<std::endl;
#endif
            throw IndexScanCompletedException();
          }
          bufMgr->unPinPage(file, currentPageNum,false);
          currentPageNum = thisPage->rightSibPageNo;
          bufMgr->readPage(file, currentPageNum,currentPageData);
          nextEntry = 0;
        }
      }

      // will not un pin this page, because it will be used later
      // or we ask for it every time we need to read data from it?
//       bufMgr->unPinPage(file, currentPageNum, false);
}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    // make sure the scan state is true;
    if ( scanExecuting == false) {
      std::cout<<"No scan started\n";
      throw ScanNotInitializedException();
    }

    if ( attributeType == INTEGER ) {
      scanNextHelper<int, struct LeafNodeInt>(outRid, lowValInt, highValInt);
    } else if ( attributeType == DOUBLE ) {
      scanNextHelper<double, struct LeafNodeDouble>(outRid, lowValDouble, highValDouble);
    } else if ( attributeType == STRING ) {
      // TODO
//       char lowVal[STRINGSIZE], highVal[STRINGSIZE];
//       const char * lowValc_str = lowValString.c_str();
//       const char * highValc_str = highValString.c_str();
//       strncpy(lowVal, (lowValc_str), STRINGSIZE);
//       strncpy(highVal, (highValc_str), STRINGSIZE);
//       strncpy(lowVal, lowStringKey, STRINGSIZE);
//       strncpy(highVal, highStringKey, STRINGSIZE);
#ifdef DEBUGSTRING
//   std::cout<<"highVal |"<<highVal<<"| at "<<__LINE__<<std::endl;
#endif
      scanNextHelper<char[STRINGSIZE], struct LeafNodeString>(outRid, lowStringKey, highStringKey);
#ifdef DEBUGSCAN
  std::cout<<"outRid.page_number is "<<outRid.page_number<<std::endl;
#endif

    } else {
       std::cout<<"Unsupported data type\n";
       exit(1);  // dangerous exit, need to clean the memory ??
    }


  // if scan reach the end of the list
//   throw IndexScanCompletedException;
}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNextHelper
// -----------------------------------------------------------------------------

template <class T, class T_LeafNode >
const void BTreeIndex::scanNextHelper(RecordId & outRid, T lowVal, T highVal)
{
#ifdef DEBUGSTRING
  std::cout<<"highVal |"<<highVal<<"| at "<<__LINE__<<std::endl;
#endif

    T_LeafNode* thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);

#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
    if ( compare<T>(thisPage->keyArray[nextEntry], highVal) > 0 ) {
#ifdef DEBUGSTRING
  std::cout<<thisPage->keyArray[nextEntry]<<std::endl;
  std::cout<<"highVal |"<<highVal<<"| at "<<__LINE__<<std::endl;
  std::cout<<" IndexScanCompletedException throwed there at "<<__LINE__<<std::endl;
#endif
      throw IndexScanCompletedException();
    } else if ( compare<T>(highVal,thisPage->keyArray[nextEntry])==0 && highOp == LT ) {
    // check with LT or LTE
#ifdef DEBUGSTRING
  std::cout<<" IndexScanCompletedException throwed there at "<<__LINE__<<std::endl;
#endif
      throw IndexScanCompletedException();
    }

#ifdef DEBUGSCAN
  std::cout<<" in scanNextHelper nextEntry is "<<nextEntry<<std::endl;
  std::cout<<" in scanNextHelper thisPage->ridArray[nextEntry].page_number is ";
  std::cout<<(thisPage->ridArray[nextEntry].page_number)<<std::endl;
#endif

    outRid = thisPage->ridArray[nextEntry];
#ifdef DEBUGSCAN
  std::cout<<"outRid.page_number is "<<outRid.page_number<<std::endl;
#endif

//       shiftToNextEntry(thisPage);
    if ( ++nextEntry >= thisPage->size ) { // move to next page
      if ( thisPage->rightSibPageNo == 0 ) { // no more pages
#ifdef DEBUGSTRING
  std::cout<<" IndexScanCompletedException throwed there at "<<__LINE__<<std::endl;
#endif
        throw IndexScanCompletedException();
      }
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = thisPage->rightSibPageNo;
      bufMgr->readPage(file, currentPageNum,currentPageData);
      nextEntry = 0;
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
  // change the scan state.
  // if there isn't as startScan
  if ( scanExecuting == false ) {
    throw ScanNotInitializedException();
  } else {
    scanExecuting = false;
  }

  bufMgr->unPinPage(file, currentPageNum, false);

  // end the scan, unpin all pages have been pinned
  //

}

}


