
// #define DEBUGDUP
// #define DEBUGINSERT
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

// #define DEBUGFINDPARENT

// #define DEBUGDELETE
// #define DEBUGGETINDEX

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
#include "exceptions/page_pinned_exception.h"
#include "exceptions/empty_btree_exception.h"

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
}



template <class T>
const void copyKey( T &a, T &b )
{
  a = b;
}


template<> // explicit specialization for T = char[STRINGSIZE]
const void copyKey<char[STRINGSIZE]>( char (&a)[STRINGSIZE], char (&b)[STRINGSIZE])
{
  strncpy(a,b,STRINGSIZE);
}


/**
 * Return the index of the first key in the given page that is larger than or
 * equal to the param key.
 * 
 * @param thisPage Leaf/NonLeaf node page.
 * @param key      Key to compare
 *
 * @return index of the first key that is not smaller than key
 */
template<class T, class T_NodeType>
const int getIndex( T_NodeType * thisPage, T &key )
{
    int size = thisPage->size;

    // no entries at all
    if ( size <= 0 ) {
#ifdef DEBUGGETINDEX
  std::cout<<"size of this node: "<<size<<"\n";
#endif
      return -1;
    }

    // 1. check if the 0th is 
    if ( compare<T>(key, thisPage->keyArray[0]) <= 0 )
      return 0;
    // check the last
    if ( compare<T>(key, thisPage->keyArray[size-1]) > 0 )
      return size;

    // 2. else binary seach
    int left = 1, right = thisPage->size-1;
    T* keyArray = thisPage->keyArray;
    int mid = right - (right - left )/2;
    // stop when [left-1]>0, [left] <= 0
    while ( ! (compare<T>(key, keyArray[mid]) <= 0
               && compare<T>(key, keyArray[mid-1]) > 0 ) ) {
      int cmp = compare<T>(key, keyArray[mid]);
      if ( cmp == 0 ) {
        return mid;
      }  else if ( cmp > 0 ) {
        left = mid + 1;
      } else {
        right = mid - 1; // it is possible that mid is the first larger
      }
      mid = right - (right-left)/2;
    }
    return mid;
}


template<class T_NodeType>
const void BTreeIndex::shiftToNextEntry(T_NodeType *thisPage)
{
    if ( ++nextEntry >= thisPage->size ) { // move to next page
      if ( thisPage->rightSibPageNo == 0 ) { // no more pages
//         throw IndexScanCompletedException();
        // haiyun test
          nextEntry = -1;
          return; // just return
      }
      PageId nextPageNo = thisPage->rightSibPageNo; 
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = nextPageNo;
      bufMgr->readPage(file, currentPageNum, currentPageData);
      nextEntry = 0;
    }
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
      intRootPage->rightSibPageNo = 0;
    } else if ( attributeType == DOUBLE ) {
      LeafNodeDouble* doubleRootPage = reinterpret_cast<LeafNodeDouble*>(tempPage);
      doubleRootPage->size = 0;
      doubleRootPage->rightSibPageNo = 0;
    } else if ( attributeType == STRING ) {
      LeafNodeString* stringRootPage = reinterpret_cast<LeafNodeString*>(tempPage);
      stringRootPage->size = 0;
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
    try {
      if ( file ) {
#ifdef DEBUG
  std::cout<<" delete file object "<<std::endl;
#endif
        // unpin
        bufMgr->flushFile(file);
        delete file;
        file = NULL;
      }
    } catch( PageNotPinnedException e) {
      std::cout<<"Flushing file with page not pinned"<<std::endl;
    } catch ( PagePinnedException e ) {
      std::cout<<"PagePinnedException"<<std::endl;
#ifdef DEBUG
//         throw e;
    } catch ( BadgerDbException e ) {
      std::cout<<"other exception"<<std::endl;
      throw e;
#endif
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
#ifdef DEBUGINSERT
  std::cout<<"  inserting "<<intKey<<std::endl;
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
  
//   int index = getIndex(thisPage, key);
  int index = getIndex<T, T_NonLeafNode>(thisPage, key);


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
  copyKey((thisKey), ((rkpair.key)));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of thisKey is ";
  std::cout<<thisKey<<" and it should be "<<rkpair.key<<std::endl;
#endif

  int size = thisPage->size;
#ifdef DEBUGMORE
  std::cout<<" Inserting to pageNo " << pageNo <<std::endl;
  std::cout<<"before insert into leaf, size of the page is "<<size<<std::endl;
#endif
  if ( size < leafOccupancy ) {
#ifdef DEBUGMORE
  std::cout<<"size is smaller than "<<leafOccupancy<<", just insert"<<std::endl;
#endif
    // Still have space, just insert into correct position
//     int index = getIndex(thisPage, thisKey);
    int index = getIndex<T, T_LeafNode>(thisPage, thisKey);

    if ( index == -1 ) index = 0;

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
#endif

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
    copyKey((copyUpKey), ((firstPage->keyArray[midIndex])));
#ifdef DEBUGCOPY
  std::cout<<"After copy key, the new value of copyUpKey is ";
  std::cout<<copyUpKey<<" and it should be "<<firstPage->keyArray[midIndex]<<std::endl;
#endif

    bool rootIsLeaf = rootPageNum == 2;

    // check if here is parent
    if ( !rootIsLeaf ) { // normal parent, insert new key and pageNo
      PageId firstPageParentNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
        (firstPageNo, firstPage->keyArray[firstPage->size-1]);
      
      // done with first and second page
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);
#ifdef DEBUGLEAF
  std::cout << "in splitleafnode, new secondPageNo is "<< secondPageNo << std::endl;
#endif
      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (firstPageParentNo, copyUpKey, secondPageNo);
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
//       int index = getIndex(thisPage, thisKey);
      int index = getIndex<T, T_NonLeafNode>(thisPage, thisKey);

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
      //  TODO  Decide where to insert, left or right
      int midIndex = nodeOccupancy/2;
//       if ( !insertLeftNode ) {
//         midIndex += nodeOccupancy%2;
//       }

      bool insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;

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

    // set the level
    secondPage->level = firstPage->level;

    // TODO decide whether the key is inserted left or right
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

      PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
        (firstPageNo, firstPage->keyArray[firstPage->size-1]);

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


template< class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::findParentOf(PageId childPageNo, T &key)
{
  Page *tempPage;

  // start from the root page and recursively
  PageId nextPageNo = rootPageNum;
  PageId parentNodeNo = 0;
#ifdef DEBUGFINDPARENT
  std::cout<<" child node no "<<childPageNo;
#endif
  while ( 1 ) {
    bufMgr->readPage(file, nextPageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    
//     int index = getIndex(thisPage, key);
    int index = getIndex<T, T_NonLeafNode>(thisPage, key);

    parentNodeNo = nextPageNo;
    nextPageNo = thisPage-> pageNoArray[index];
    bufMgr->unPinPage(file, parentNodeNo, false);

    if ( nextPageNo == childPageNo )  {
      break; // found
    }
  }

#ifdef DEBUGFINDPARENT
  std::cout<<" parent found "<<parentNodeNo<<std::endl;
#endif

  return parentNodeNo;
}



const void BTreeIndex::printTree()
{
  switch  (attributeType ) {
    case INTEGER:
      printTree<int, struct NonLeafNodeInt, struct LeafNodeInt>();
      break;
    case DOUBLE:
      printTree<double, struct NonLeafNodeDouble, struct LeafNodeDouble>();
      break;
    case STRING:
      printTree<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>();
      break;
    default:
      std::cout<<"Unsupported data type\n";
  }
}



template <class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::printTree() 
{

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
// BTreeIndex::deleteEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::deleteEntry(const void* key)
{

#ifdef DEBUGDELETE
  std::cout<<"Deleting entry\n";
  std::cout<<" rootPageNum is "<<rootPageNum<<"\n";

#endif
    // determine the type
    if ( attributeType == INTEGER ) {
        int intKey = *(int*)key;
        PageId leafToDelete = findLeafNode<int, struct NonLeafNodeInt>
          (rootPageNum, intKey);
        deleteLeafNode<int, struct NonLeafNodeInt, struct LeafNodeInt>
          (leafToDelete, intKey);
    } else if ( attributeType == DOUBLE ) {
        double doubleKey = *(double*)key;
        PageId leafToDelete = findLeafNode<double, struct NonLeafNodeDouble>
          (rootPageNum, doubleKey);
        deleteLeafNode<double, struct NonLeafNodeDouble, struct LeafNodeDouble>
          (leafToDelete, doubleKey);
    } else if ( attributeType == STRING ) {
        char stringKey[STRINGSIZE];
        strncpy(stringKey, (char*)key, STRINGSIZE);
        PageId leafToDelete = findLeafNode<char[STRINGSIZE], struct NonLeafNodeString>
          (rootPageNum, stringKey);
        deleteLeafNode<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>
          (leafToDelete, stringKey);
    }
}



// -----------------------------------------------------------------------------
// BTreeIndex::deleteLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::deleteLeafNode(PageId pageNo, T& key)
{
  Page * tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_LeafNode * thisPage = reinterpret_cast<T_LeafNode*>(tempPage);

  int index = getIndex<T, T_LeafNode>(thisPage, key);

  if ( index == -1 ) {
    bufMgr->unPinPage(file, pageNo, false);
    throw EmptyBTreeException();
  }

  // check if this key is equal to the key to be deleted. 
  // If not, key does not exist in the tree, just return.
  if ( compare(thisPage->keyArray[index], key) != 0 ) {
    std::cout<<"key does not exist!\n";
    bufMgr->unPinPage(file, pageNo, false);
    return;
  }

  // delete one anyway
  (thisPage->size)--;
  int thisSize = thisPage->size;
  memmove((void*)(&(thisPage->keyArray[index])),
          (void*)(&(thisPage->keyArray[index+1])), sizeof(T)*(thisSize-index));
  memmove((void*)(&(thisPage->ridArray[index])),
          (void*)(&(thisPage->ridArray[index+1])), sizeof(RecordId)*(thisSize-index));

  int leafHalfFillNo = leafOccupancy/2;


  // size of node in  [leafOccupancy/2, leafOccupancy]
  // Exception: root page does not need to be at least half filled
  // return if it is root or size is large enough
  if ( pageNo == rootPageNum || thisSize >= leafHalfFillNo ) {
    bufMgr->unPinPage(file, pageNo, true);
#ifdef DEBUGDELETE
  std::cout<<" case 0: deletion without adjust from page "<<pageNo<<"\n";
#endif
    return;
  }
 


#ifdef DEBUGDELETE
  std::cout<<" try redistribute or merge\n";
#endif
  { // need to redistribution or merge
    // check if the neigbours have extra, check both neigbour
    // If this is the last child, need to find the previous child, with a bit pain.

    bool needToMerge = false; // need to merge or not
    PageId firstPageNo = 0, secondPageNo = 0;


    PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
      (pageNo, thisPage->keyArray[thisSize-1]);

//     PageId firstPageParentNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
//       (firstPageNo, firstPage->keyArray[firstPage->size-1]);

#ifdef DEBUGDELETE
  std::cout<<" parent is " <<parentPageNo<<"\n";
#endif

    bufMgr->readPage(file, parentPageNo, tempPage);
    T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

    // easier case first, check right sibling
    PageId rightPageNo = thisPage->rightSibPageNo;


#ifdef DEBUGDELETE
  std::cout<<" right page number is " <<rightPageNo<<"\n";
#endif


    if ( rightPageNo != 0 ) { // try to borrow one from right sibling
#ifdef DEBUGDELETE
  std::cout<<" try borrow from right sibling\n";
#endif
      bufMgr->readPage(file, rightPageNo, tempPage);
      T_LeafNode* rightPage = reinterpret_cast<T_LeafNode*>(tempPage);
      int rightPageSize = rightPage->size;
      if ( rightPageSize > leafHalfFillNo ) { // just borrow one, change parent key, done!
        // update the parent key
//         int pindex = getIndex(parentPage, rightPage->keyArray[0]);
        int pindex = getIndex<T, T_NonLeafNode>(parentPage, rightPage->keyArray[0]);
        copyKey(parentPage->keyArray[pindex], rightPage->keyArray[1]);
        // copy one from right to left
        copyKey(thisPage->keyArray[thisSize], rightPage->keyArray[0]);
        thisPage->ridArray[thisSize] = rightPage->ridArray[0];
        // delete the original one
        memmove((void*)(&(rightPage->keyArray[0])),
                (void*)(&(rightPage->keyArray[1])), sizeof(T)*(rightPageSize-1));
        memmove((void*)(&(rightPage->ridArray[0])),
                (void*)(&(rightPage->ridArray[1])), sizeof(RecordId)*(rightPageSize-1));
        (rightPage->size)--;
        (thisPage->size)++;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, rightPageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
#ifdef DEBUGDELETE
  std::cout<<" case 1.1: redistribute from right sibling\n";
#endif
        return;
      } else {// unfortunately, merge.
        needToMerge = true;
        firstPageNo = pageNo;
        secondPageNo = rightPageNo;
        bufMgr->unPinPage(file, rightPageNo, false);
#ifdef DEBUGDELETE
  std::cout<<" right page don't have enough item, may want merge ";
  std::cout<<"page "<< firstPageNo<<" and " << secondPageNo<<"\n";
#endif
      }
    }

    // do extra work to retreive the left sibling
//     int pindex = getIndex(parentPage, thisPage->keyArray[thisSize-1]);
    int pindex = getIndex<T, T_NonLeafNode>(parentPage, thisPage->keyArray[thisSize-1]);
    // if the current page is already the first, pindex is 0
    if ( pindex > 0 ) { // try to borrow one from left sibling
#ifdef DEBUGDELETE
  std::cout<<" try borrow from left sibling\n";
#endif

#ifdef DEBUGDELETE
  std::cout<<" pindex is "<<pindex<<"\n";
#endif
      PageId leftPageNo = parentPage->pageNoArray[pindex-1];
      bufMgr->readPage(file, leftPageNo, tempPage);
      T_LeafNode* leftPage = reinterpret_cast<T_LeafNode*>(tempPage);
      int leftPageSize = leftPage->size;

      if ( leftPageSize > leafHalfFillNo ) { // just borrow one
        // update the parent
        copyKey(parentPage->keyArray[pindex], leftPage->keyArray[leftPageSize-1]);
        // shift all keys in this page to make room for incoming key
        memmove((void*)(&(thisPage->keyArray[1])),
                (void*)(&(thisPage->keyArray[0])), sizeof(T)*(thisSize));
        memmove((void*)(&(thisPage->ridArray[1])),
                (void*)(&(thisPage->ridArray[0])), sizeof(RecordId)*(thisSize));
        // copy one from left to right
        copyKey(thisPage->keyArray[0], leftPage->keyArray[leftPageSize-1]);
        thisPage->ridArray[0] = leftPage->ridArray[leftPageSize-1];
        (leftPage->size)--;
        (thisPage->size)++;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, leftPageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
#ifdef DEBUGDELETE
  std::cout<<" case 1.2: redistribute from left sibling\n";
#endif
        return;
      } else {
//         needToMerge = true;
//         firstPageNo = leftPageNo;
//         secondPageNo = pageNo;
        bufMgr->unPinPage(file, leftPageNo, false);
#ifdef DEBUGDELETE
  std::cout<<" no enough item in left, must merge ";
  std::cout<<"page "<< firstPageNo<<" and " << secondPageNo<<"\n";
#endif
      }
    }
    bufMgr->unPinPage(file, pageNo, true);

    // merge if redistribution fails
    if ( needToMerge ) {
#ifdef DEBUGDELETE
  std::cout<<" case 2: merge ";
  std::cout<<"page "<< firstPageNo<<" and " << secondPageNo<<"\n";
#endif
//       mergeLeafNode<T, T_NonLeafNode, T_LeafNode>(firstPageNo, secondPageNo);
      bufMgr->readPage(file, firstPageNo, tempPage);
      T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
      bufMgr->readPage(file, secondPageNo, tempPage);
      T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

      int size1 = firstPage->size, size2 = secondPage->size;
      // validate
      if ( size1+size2 > leafOccupancy) {
#ifdef DEBUGDELETE
#endif
        std::cout<<" final size larger than leafOccupancy after merging!\n";
        return;
      }

      // combine entries
      memmove((void*)(&( firstPage->keyArray[size1])),
              (void*)(&(secondPage->keyArray[0])), sizeof(T)*(size2));
      memmove((void*)(&( firstPage->ridArray[size1])),
              (void*)(&(secondPage->ridArray[0])), sizeof(RecordId)*(size2));
      firstPage->size = size1+size2;

      // delete the second page
      firstPage->rightSibPageNo = secondPage->rightSibPageNo;
      bufMgr->unPinPage(file, secondPageNo, false);
//       bufMgr->disposePage(file, secondPageNo); // TODO

      // delete parent entry
      T key;
      copyKey(key, firstPage->keyArray[0]);
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, parentPageNo, true);
      deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, key);
    } else {
      bufMgr->unPinPage(file, parentPageNo, true);
    }
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::mergeLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::mergeLeafNode(PageId firstPageNo, PageId secondPageNo)
{
  Page* tempPage;
  bufMgr->readPage(file, firstPageNo, tempPage);
  T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
  bufMgr->readPage(file, secondPageNo, tempPage);
  T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

  int size1 = firstPage->size, size2 = secondPage->size;

  // validate
  if ( size1+size2 > leafOccupancy) {
#ifdef DEBUGDELETE
#endif
    std::cout<<" final size larger than leafOccupancy after merging!\n";
    return;
  }

  // combine entries
  memmove((void*)(&( firstPage->keyArray[size1])),
          (void*)(&(secondPage->keyArray[0])), sizeof(T)*(size2));
  memmove((void*)(&( firstPage->ridArray[size1])),
          (void*)(&(secondPage->ridArray[0])), sizeof(RecordId)*(size2));
  firstPage->size = size1+size2;

  // delete the second page
  firstPage->rightSibPageNo = secondPage->rightSibPageNo;
  bufMgr->unPinPage(file, secondPageNo, false);
//   bufMgr->disposePage(file, secondPageNo); // TODO lead to invaildpagee

  // delete parent entry
  PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
    (firstPageNo, firstPage->keyArray[size1-1]);
  T key;
  copyKey(key, firstPage->keyArray[0]);
  bufMgr->unPinPage(file, firstPageNo, true);
  deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, key);
//   deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, index);
}



// -----------------------------------------------------------------------------
// BTreeIndex::deleteNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::deleteNonLeafNode(PageId pageNo, T& key)
{
  Page* tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

//   int index = getIndex(thisPage, key);
  int index = getIndex<T, T_NonLeafNode>(thisPage, key);
  // delete the key at index
  (thisPage->size)--;
  int thisSize = thisPage->size;
  if ( index == thisSize ) { // if it is the last key to be delete, done.
  } else { // otherwise, need to shift all keys left by one
    memmove((void*)(&(thisPage->keyArray[index])),
            (void*)(&(thisPage->keyArray[index+1])), sizeof(T)*(thisSize-index));
    memmove((void*)(&(thisPage->pageNoArray[index+1])),
            (void*)(&(thisPage->pageNoArray[index+2])), sizeof(PageId)*(thisSize-index));
  }


  // check if we need to cut down the tree
  if ( pageNo == rootPageNum && thisSize == 0 ) {
#ifdef DEBUGDELETE
  std::cout<<"changing rootPageNum from "<<rootPageNum;
  std::cout<<thisPage->pageNoArray[0]<<std::endl;
#endif
    // cut down tree by 1 level
    // update class field
    rootPageNum = thisPage->pageNoArray[0];
    // update meta page
    bufMgr->readPage(file, headerPageNum, tempPage);
    IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
    metaPage->rootPageNo = thisPage->pageNoArray[0];
    bufMgr->unPinPage(file, headerPageNum, true);
    
    // delete current page
    bufMgr->unPinPage(file, pageNo, false);
//     bufMgr->disposePage(file, pageNo);
    return;
  }

  // determine if we need to redistribute or merge nodes
  int nodeHalfFillNo = nodeOccupancy/2;
  if ( pageNo == rootPageNum || thisSize >= nodeHalfFillNo ) {
    bufMgr->unPinPage(file, pageNo, true);
    return;
  }

  // decide how to adjust the tree
  {
    bool needToMerge = false;
    PageId firstPageNo = 0, secondPageNo = 0;

    PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
      (pageNo, thisPage->keyArray[thisSize-1]);
    bufMgr->readPage(file, parentPageNo, tempPage);
    T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

    // there is not shortcut to find sibling nodes, use the painful way as in leaf node

    //                                                use the last one as index
    //                                                to get pointer that points
    //                                                to current page
//     int pindex = getIndex(parentPage, thisPage->keyArray[thisSize-1]);
    int pindex = getIndex<T, T_NonLeafNode>(parentPage, thisPage->keyArray[thisSize-1]);
    // pindex is in [1,size], guaranteed to have previous one, but not next one

    if ( pindex < parentPage->size ) { // try right page 
      // next index points to the right child
      PageId rightPageNo = parentPage->pageNoArray[pindex+1];
      bufMgr->readPage(file, rightPageNo, tempPage);
      T_NonLeafNode* rightPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
      int rightPageSize = rightPage->size;

      if ( rightPageSize > nodeHalfFillNo ) { // just borrow one
        // 1. move the parent key and the first pointer in right page to this page
        copyKey(thisPage->keyArray[thisSize], parentPage->keyArray[pindex]);
        thisPage->pageNoArray[thisSize+1] = rightPage->pageNoArray[0];
        // 2. move right page key to the parent
        copyKey(parentPage->keyArray[pindex], rightPage->keyArray[0]);
        // 3. shift all data in right page
        memmove((void*)(&(rightPage->keyArray[0])),
                (void*)(&(rightPage->keyArray[1])), sizeof(T)*(rightPageSize-1));
        memmove((void*)(&(rightPage->pageNoArray[0])),
                (void*)(&(rightPage->pageNoArray[1])), sizeof(PageId)*(rightPageSize));

        (thisPage->size)++;
        (rightPage->size)--;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
        bufMgr->unPinPage(file, rightPageNo, true);
        return;
      } else { // need to merge
        needToMerge = true;
        firstPageNo = pageNo;
        secondPageNo = rightPageNo;
        bufMgr->unPinPage(file, rightPageNo, false);
      }
    }

    { // try left page
      // the previous one is the left child
      PageId leftPageNo = parentPage->pageNoArray[pindex-1];
      bufMgr->readPage(file, leftPageNo, tempPage);
      T_NonLeafNode* leftPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
      int leftPageSize = leftPage->size;

      if ( leftPageSize > nodeHalfFillNo ) { // just borrow one
        // 0. make room for new key in this page
        memmove((void*)(&(thisPage->keyArray[1])),
                (void*)(&(thisPage->keyArray[0])), sizeof(T)*(thisSize));
        memmove((void*)(&(thisPage->pageNoArray[1])),
                (void*)(&(thisPage->pageNoArray[0])), sizeof(PageId)*(thisSize+1));
        // 1. move the parent key to this page, and last pointer in left page to this page
        copyKey(thisPage->keyArray[0], parentPage->keyArray[pindex]);
        thisPage->pageNoArray[0] = leftPage->pageNoArray[leftPageSize];
        // 2. move the last key in left to parent
        copyKey(parentPage->keyArray[pindex], leftPage->keyArray[leftPageSize-1]);
        (thisPage->size)++;
        (leftPage->size)--;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
        bufMgr->unPinPage(file, leftPageNo, true);
        return;
      } else { // need o Merge
        needToMerge = true;
        firstPageNo = leftPageNo;
        secondPageNo = pageNo;
        bufMgr->unPinPage(file, leftPageNo, false);
      }
    }

    bufMgr->unPinPage(file, pageNo, true);
    bufMgr->unPinPage(file, parentPageNo, true);


    // merge if redistribution fails
    if (needToMerge) 
      mergeNonLeafNode<T, T_NonLeafNode, T_LeafNode>(firstPageNo, secondPageNo);

  }

}


// -----------------------------------------------------------------------------
// BTreeIndex::mergeNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::mergeNonLeafNode(PageId firstPageNo, PageId secondPageNo)
{
  Page* tempPage;
  bufMgr->readPage(file, firstPageNo, tempPage);
  T_NonLeafNode* firstPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  bufMgr->readPage(file, secondPageNo, tempPage);
  T_NonLeafNode* secondPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

  int size1 = firstPage->size, size2 = secondPage->size;
  // validate
  if ( size1+size2 > nodeOccupancy) {
#ifdef DEBUGDELETE
#endif
    std::cout<<" final size larger than nodeOccupancy after merging!\n";
    return;
  }


  PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
    (firstPageNo, firstPage->keyArray[size1-1]);
  bufMgr->readPage(file, parentPageNo, tempPage);
  T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

  // there is not shortcut to find sibling nodes, use the painful way as in leaf node

  //                                                use the last one as index
  //                                                to get pointer that points
  //                                                to current page
//   int pindex = getIndex(parentPage, firstPage->keyArray[size1-1]);
  int pindex = getIndex<T, T_NonLeafNode>(parentPage, firstPage->keyArray[size1-1]);

  // combine entries
  copyKey(firstPage->keyArray[size1], parentPage->keyArray[pindex]);
  memmove((void*)(&( firstPage->keyArray[size1+1])),
          (void*)(&(secondPage->keyArray[0])), sizeof(T)*(size2));
  memmove((void*)(&( firstPage->pageNoArray[size1+1])),
          (void*)(&(secondPage->pageNoArray[0])), sizeof(RecordId)*(size2+1));
  firstPage->size = size1+size2+1; // one from the parent page

  // delete the second page
  bufMgr->unPinPage(file, secondPageNo, false);
//   bufMgr->disposePage(file, secondPageNo);

  // delete parent entry
  T key;
  copyKey(key, firstPage->keyArray[0]);
  bufMgr->unPinPage(file, firstPageNo, true);
  deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, key);
//   deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, index);
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
      // findLeafNode method only find the page that key shoud be insert in,
      // which means if the lowVal is equal to the first key in some page,
      // the returned page is the previous page
      currentPageNum = findLeafNode<T, T_NonLeafNode>(rootPageNum, lowVal);
      // find the first index
#ifdef DEBUGSCAN
  std::cout<<" in startScanHelper, currentPageNum is "<<currentPageNum<<std::endl;
#endif
      bufMgr->readPage(file, currentPageNum, currentPageData);
      T_LeafNode* thisPage;
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
      // else search the nextEntry
      if ( compare<T>(lowVal, thisPage->keyArray[size-1]) > 0 ) {
        nextEntry = size-1;
        shiftToNextEntry<T_LeafNode>(thisPage);
        thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      } else {
//         nextEntry = getIndex(thisPage, lowVal);
        nextEntry = getIndex<T, T_LeafNode>(thisPage, lowVal);
      }

#ifdef DEBUGSCAN
  std::cout<<" in startScanHelper, currentPageNum is "<<currentPageNum<<std::endl;
  std::cout<<" in startScanHelper, nextEntry is "<<nextEntry<<std::endl;
#endif

#ifdef DEBUGCOMPAREOK
  std::cout<<" ooooooooo call compare at "<<__LINE__<<" oooooooo" <<std::endl;
#endif
      // check with the lowOp, if open and current happen to equal, shift next
      if ( lowOp==GT && compare<T>(lowVal, thisPage->keyArray[nextEntry])==0) {
        shiftToNextEntry<T_LeafNode>(thisPage);
      }

      // will not unpin this page, because it will be used later
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

// haiyun test
    if ( nextEntry == -1 ) 
      throw IndexScanCompletedException();

    T_LeafNode* thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);

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
    
      shiftToNextEntry<T_LeafNode>(thisPage);
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

  // unpin the current scanning page
  bufMgr->unPinPage(file, currentPageNum, false);

}

}

