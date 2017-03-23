
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

namespace badgerdb
{


template<class T>
const int compare(const T a, const T b)
{
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

template<> // explicit specialization for T = void
const void copyKey<char[STRINGSIZE]>( char (&a)[STRINGSIZE], char (&b)[STRINGSIZE])
{
  strncpy(a,b,STRINGSIZE);
}

template<class T, class T_NodeType>
const int getIndex( T_NodeType * thisPage, T &key )
{
    int size = thisPage->size;
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
        throw IndexScanCompletedException();
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

    scanExecuting = false;
    nextEntry = -1;
    currentPageNum = 0;

  // 2. check if the index file exits. open or create new
  //   if it is new, create metanode, create the root node
  try { // try read old file
    file = new BlobFile(outIndexName,false);
    Page *tempPage;
    headerPageNum = file->getFirstPageNo();
    bufMgr->readPage(file, headerPageNum, tempPage);
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(tempPage);
    rootPageNum = metaInfo->rootPageNo;

    bufMgr->unPinPage(file, headerPageNum, false);
    if ( relationName.compare(metaInfo->relationName) != 0
        || metaInfo->attrByteOffset != attrByteOffset
        || metaInfo->attrType != attrType
       ) {
      // meta info does not match in index file, clear and return;
      std::cout<<"Meta info does not match the index!\n";
      delete file;
      file = NULL;
      return;
    }
  } catch (FileNotFoundException e ) {
    // This is a new index file, create and contruct the new index file
    Page *tempPage;
    file = new BlobFile(outIndexName, true);

    bufMgr->allocPage(file, headerPageNum, tempPage);
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(tempPage);
    bufMgr->allocPage(file, rootPageNum, tempPage);

    // assign index meta info
    std::copy(relationName.begin(), relationName.end(), metaInfo->relationName);
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;

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

    // build B-Tree: insert the record into the B-Tree
    buildBTree(relationName);
  }
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
      }
    } catch(EndOfFileException e) {
    } 
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
    
    if ( currentPageNum ) {
      try {
        bufMgr->unPinPage(file, currentPageNum, false);
      } catch ( PageNotPinnedException e) {
      }
    }

    scanExecuting = false;
    try {
      if ( file ) {
        // unpin
        bufMgr->flushFile(file);
        delete file;
        file = NULL;
      }
    } catch( PageNotPinnedException e) {
      std::cout<<"Flushing file with page not pinned"<<std::endl;
    } catch ( PagePinnedException e ) {
      std::cout<<"PagePinnedException"<<std::endl;
    } 
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
  if ( rootPageNum == 2 ) { // root is leaf
    return rootPageNum; 
  }

  // pageNo should points to a non-leaf node page
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  
  int index = getIndex<T, T_NonLeafNode>(thisPage, key);

  PageId leafNodeNo = thisPage->pageNoArray[index];
  int thisPageLevel  = thisPage->level;
  bufMgr->unPinPage(file, pageNo, false);
  if ( thisPageLevel == 0 ) { // next level is non-leaf node
    leafNodeNo = findLeafNode<T, T_NonLeafNode>(leafNodeNo, key);
  }

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

  int size = thisPage->size;
  if ( size < leafOccupancy ) {
    // Still have space, just insert into correct position
    int index = getIndex<T, T_LeafNode>(thisPage, thisKey);

    // use memmove
    memmove((void*)(&(thisPage->keyArray[index+1])),
            (void*)(&(thisPage->keyArray[index])), sizeof(T)*(size-index));
    memmove((void*)(&(thisPage->ridArray[index+1])),
            (void*)(&(thisPage->ridArray[index])), sizeof(RecordId)*(size-index));

    // insert the current key
    copyKey(((thisPage->keyArray[index])), (thisKey));

    thisPage->ridArray[index] = rkpair.rid;

    // increase the size by 1;
    (thisPage->size)++;
    bufMgr->unPinPage(file, pageNo, true);
  } else {
    // not enough space, need to split

    // determine where this rkpair should go
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    bool insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;

    // close the page to split
    bufMgr->unPinPage(file, pageNo, false);

    // split the node and get the newly allocated PageId
    PageId rightPageNo = splitLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNo);

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
    bufMgr->allocPage(file, secondPageNo, tempPage);
    T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

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

    // check if here is parent
    bool rootIsLeaf = rootPageNum == 2;
    if ( !rootIsLeaf ) { // normal parent, insert new key and pageNo
      PageId firstPageParentNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
        (firstPageNo, firstPage->keyArray[firstPage->size-1]);
      
      // done with first and second page
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);
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
    int size = thisPage->size;
    if ( size < nodeOccupancy ) {
      // still have space, just insert
      // find the correct position to insert the key
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

    bool currentNodeIsRoot = rootPageNum == firstPageNo;

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
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;
      
      // done with parentPage, save changes
      bufMgr->unPinPage(file, parentPageNo, true);
    }

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
  while ( 1 ) {
    bufMgr->readPage(file, nextPageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    
    int index = getIndex<T, T_NonLeafNode>(thisPage, key);

    parentNodeNo = nextPageNo;
    nextPageNo = thisPage-> pageNoArray[index];
    bufMgr->unPinPage(file, parentNodeNo, false);

    if ( nextPageNo == childPageNo )  {
      break; // found
    }
  }

  return parentNodeNo;
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
      strncpy(lowStringKey, (char*)lowValParm, STRINGSIZE);
      strncpy(highStringKey, (char*)highValParm, STRINGSIZE);
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
      // which means it the lowVal is equal to the first key in some page,
      // the returned page is the previous page
      currentPageNum = findLeafNode<T, T_NonLeafNode>(rootPageNum, lowVal);
      // find the first index
      bufMgr->readPage(file, currentPageNum, currentPageData);
      T_LeafNode* thisPage;
      thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      
      int size = thisPage->size;

      // if lowValInt is larger than the last entry, it should goto next page
      // else search the nextEntry
      if ( compare<T>(lowVal, thisPage->keyArray[size-1]) > 0 ) {
        nextEntry = size-1;
        shiftToNextEntry<T_LeafNode>(thisPage);
        thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      } else {
        nextEntry = getIndex<T, T_LeafNode>(thisPage, lowVal);
      }

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
      scanNextHelper<char[STRINGSIZE], struct LeafNodeString>(outRid, lowStringKey, highStringKey);

    } else {
       std::cout<<"Unsupported data type\n";
       exit(1);  // dangerous exit, need to clean the memory ??
    }

}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNextHelper
// -----------------------------------------------------------------------------

template <class T, class T_LeafNode >
const void BTreeIndex::scanNextHelper(RecordId & outRid, T lowVal, T highVal)
{
    T_LeafNode* thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);

    if ( compare<T>(thisPage->keyArray[nextEntry], highVal) > 0 ) {
      throw IndexScanCompletedException();
    } else if ( compare<T>(highVal,thisPage->keyArray[nextEntry])==0 && highOp == LT ) {
    // check with LT or LTE
      throw IndexScanCompletedException();
    }

    outRid = thisPage->ridArray[nextEntry];
    shiftToNextEntry<T_LeafNode>(thisPage);
}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

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

