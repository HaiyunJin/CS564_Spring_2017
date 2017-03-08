
#define DEBUG

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



namespace badgerdb
{

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

  // 1. check if the index file exits. open or create new
  //   if it is new, create metanode, create the root node
  try { // try read old file
#ifdef DEBU
std::cout<<"Reading old index file!!!\n";
#endif
    file = new BlobFile(outIndexName,false);
    Page *headerPage;
    headerPageNum = file->getFirstPageNo();
    bufMgr->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo *metaInfo = (IndexMetaInfo*) headerPage;
//     IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(headerPage);
    rootPageNum = metaInfo->rootPageNo;
#ifdef DEBUG
  std::cout<<"headerPageNum: "<<headerPageNum<<std::endl;
  std::cout<<"relationName: "<<relationName<<std::endl;
  std::cout<<"metaInfo->relationName: "<<metaInfo->relationName<<std::endl;
  std::cout<<"attrByteOffset: "<<attrByteOffset<<std::endl;
  std::cout<<"metaInfo->attrByteOffset: "<<metaInfo->attrByteOffset<<std::endl;
  std::cout<<"attrType: "<<attrType<<std::endl;
  std::cout<<"metaInfo->attrType: "<<metaInfo->attrType<<std::endl;
  std::cout<<"rootPageNum: "<<rootPageNum<<std::endl;
  std::cout<<"metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif

    bufMgr->unPinPage(file, headerPageNum, false);
    if ( relationName.compare(metaInfo->relationName) != 0
        || metaInfo->attrByteOffset != attrByteOffset
        || metaInfo->attrType != attrType
       ) {
      // meta info does not match in index file, clear and return;
//       delete file;
      std::cout<<"Meta info does not match the index!\n";
      return;
    }
  } catch (FileNotFoundException e ) {
    // This is a new index file, create and contruct the new index file
#ifdef DEBUG
std::cout<<"Creating new index file!!!\n";
#endif
    Page *headerPage, *rootPage;
    file = new BlobFile(outIndexName, true);
    bufMgr->allocPage(file, headerPageNum, headerPage);
    bufMgr->allocPage(file, rootPageNum, rootPage);


    // assign index meta info
//     IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(headerPage);
    std::copy(relationName.begin(), relationName.end(), metaInfo->relationName);
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;
    // done with meta page
    bufMgr->unPinPage(file, headerPageNum, true);

    // construct root page
    switch ( attributeType ) {
    case INTEGER:
      LeafNodeInt* intRootPage = reinterpret_cast<LeafNodeInt*>(rootPage);
      intRootPage->size = 0;
      intRootPage->parentPageNo = -1;
      intRootPage->rightSibPageNo = -1;
      break;
    case DOUBLE:
      LeafNodeDouble* doubleRootPage = reinterpret_cast<LeafNodeDouble*>(rootPage);
      doubleRootPage->size = 0;
      doubleRootPage->parentPageNo = -1;
      doubleRootPage->rightSibPageNo = -1;
      break;
    case STRING:
      LeafNodeString* stringRootPage = reinterpret_cast<LeafNodeString*>(rootPage);
      stringRootPage->size = 0;
      stringRootPage->parentPageNo = -1;
      stringRootPage->rightSibPageNo = -1;
      break;
    default:
      std::cout<<"Unsupported data type\n";
    }
    // done with root page
    bufMgr->unPinPage(file, rootPageNum, true);

#ifdef DEBUG
  std::cout<<"Before flush: metaInfo->relationName: "<<metaInfo->relationName<<std::endl;
  std::cout<<"Before flush: metaInfo->attrByteOffset: "<<metaInfo->attrByteOffset<<std::endl;
  std::cout<<"Before flush: metaInfo->attrType: "<<metaInfo->attrType<<std::endl;
  std::cout<<"Before flush: metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif
    bufMgr->flushFile(file);

    // build B-Tree: insert the record into the B-Tree
    // TODO rootPage
    buildBTree(relationName);

#ifdef DEBUG
  std::cout<<"headerPageNum: "<<headerPageNum<<std::endl;
  std::cout<<"rootPageNum: "<<rootPageNum<<std::endl;
#endif
  }

  // 2. set up the BTreeIndex data field
  switch  (attrType ) {
    case INTEGER:
      leafOccupancy = INTARRAYLEAFSIZE;
      nodeOccupancy = INTARRAYNONLEAFSIZE;
#ifdef DEBUG
std::cout<<"integer type\n";
#endif
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
    std::cout<<"File name of index: "<<file->filename()<<std::endl;
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
        if ( attributeType == INTEGER ) {
          int key = *((int *)(record + attrByteOffset));
#ifdef DEBUG
//             std::cout << "Extracted : " << key << std::endl;
#endif
            insertEntry(&key, scanRid);
        }
        else if ( attributeType == DOUBLE ) {
          double key = *((double *)(record + attrByteOffset));
#ifdef DEBUG
//             std::cout << "Extracted : " << key << std::endl;
#endif
            insertEntry(&key, scanRid);
        }
        else if ( attributeType == STRING ) {
          char strKey[10]; // use first 10 as key
          char* str = ((char *)(record + attrByteOffset));
          for (int i = 0 ; i < 10 ; ++i)
            strKey[i] = str[i];
          std::string key = std::string(strKey);
#ifdef DEBUG
//             std::cout << "Extracted : " << key << std::endl;
#endif
            insertEntry(&key, scanRid);
        } else {
          std::cout<<"Unsupported data type\n";
        }
      }
    } catch(EndOfFileException e) {
      std::cout << "Read all records" << std::endl;
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
  
    // delete the rkpair in the BTree  

    scanExecuting = false;
    // unpin
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  // if root is leaf, it has page num of 2
  bool rootIsLeaf = rootPageNum == 2;
  if ( rootIsLeaf ) {
    // insert into the root node, also leaf node
    switch ( attributeType ) {
      case INTEGER:
        RIDKeyPair<int> *rkpair = new RIDKeyPair<int>();
        rkpair->set(rid, *(int*)key);
        insertLeafNode<int, struct LeafNodeInt>(rootPageNum, rkpair);
        break;
      case DOUBLE:
        RIDKeyPair<double> rkpair = new RIDKeyPair<double>();
        rkpair->set(rid, *(double*)key);
        insertLeafNode<double, struct LeafNodeDouble>(rootPageNum, rkpair);
        break;
      case STRING:
        ridkeypair<std::string> rkpair = new ridkeypair<std::string>();
        char chark[10];
        strncpy(chark,(char*)key,10);
        std::string *stringkey = new std::string(chark);
        rkpair->set(rid, *stringkey);
        insertLeafNode(rootPageNum, rkpair);
        break;
      default:
        std::cout<<"Unsupported data type\n";
        break;
    }
  } else  {
    // determine the type, find the leaf node to insert, and insert
    if ( attributeType == INTEGER ) {
        RIDKeyPair<int> rkpair;
        int *intKey = (int*)key;
        rkpair.set(rid, *intkey);
        PageId leafToInsert = 
          findLeafNode<int, struct NonLeafNodeInt>(rootPageNum,intkey);
        insertLeafNode(leafToInsert, rkpair);
    } else if ( attributeType == DOUBLE ) {
        RIDKeyPair<double> rkpair;
        double *doubleKey = (double*)key;
        rkpair.set(rid, *doubleKey);
        PageId leafToInsert =
          findLeafNode<double, struct NonLeafNodeDouble>(rootPageNum,doubleKey);
        insertLeafNode(leafToInsert, rkpair);
    } else if ( attributeType == STRING ) {
        ridkeypair<std::string> rkpair;
        char chark[10];
        strncpy(chark,(char*)key,10);
        std::string *stringkey = new std::string(chark);
        rkpair.set(rid, *stringkey);
        pageid leaftoinsert = 
          findleafnode<std::string, struct nonleafnodestring>(rootPageNum,stringkey);
        insertLeafNode(leaftoinsert, rkpair);
    } else {
        std::cout<<"Unsupported data type\n";
    }
  }

}


// -----------------------------------------------------------------------------
// BTreeIndex::findLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode>
const PageId BTreeIndex::findLeafNode(PageId pageNo, const T *key)
{
  // return self if it is a leaf node. but how do I know myself is leaf?
  // Good question

  // pageNo should points to a non-leaf node page
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* rootPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  
  int index = nodeOccupancy;
  for ( int i = 0 ; i < nodeOccupancy ; ++i ) {
    if ( *key < rootPage->keyArray[i] ) {
      index = i;
      break;
    }
  }

  PageId leafNode;
  if ( rootPage->level == 0 ) { // next level is non-leaf node
    PageId nextPage = rootPage->pageNoArray[index];
    leafNode = findLeafNode<T, T_NonLeafNode>(nextPage, key);
  } else { // next level is leaf node
    leafNode = rootPage->pageNoArray[index];
  }
  bufMgr->unPinPage(file, pageNo, false);
  return leafNode;

}



// -----------------------------------------------------------------------------
// BTreeIndex::insertLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode> 
const void BTreeIndex::insertLeafNode(PageId pageNo,
                                      const RIDKeyPair<T>* rkpair)
{
  // root and leaf do not have to be at least half full

  // how to know how many entries in the node??
  // Add a new field size to all

  Page *tempPage; // temperary point, can be reused after cast
  bufMgr->readPage(file, PageNo, tempPage);
  T_LeafNode * leafPage = reinterpret_cast<T_LeafNode*>(tempPage);
  T thisKey = rkpair->key;
  int size = leafPage->size;
  if ( size < leafOccupancy ) {
    // Still have space, just insert into correct position
    int index = 0;
    // find the index that this record should be
    while ( thisKey > leafPage->keyArray[index] ) ++index;
    // shift all keys right by 1
    for ( int i = size ; i > index ; --i ) {
      leafPage->keyArray[i] = leafPage->keyArray[i-1];
      leafPage->ridArray[i] = leafPage->ridArray[i-1];
    }
    // insert the current key
    leafPage->keyArray[index] = thisKey;
    leafPage->ridArray[index] = rkpair->rid;
    // increase the size by 1;
    (leafPage->size)++;
    bufMgr->unPinPage(file, PageNo, true);
  } else {
    // No more space for new key, need to split
    bufMgr->unPinPage(file, PageNo, false);

    // split the node and insert to correct leaf
    splitLeafNode(pageNo);

    // The whole tree may changed, need to re-read the node
    bufMgr->readPage(file, PageNo, tempPage);
    leafPage = reinterpret_cast<T_LeafNode*>(tempPage);

    PageId rightPageNo = leafPage->rightSibPageNo;
    bufMgr->readPage(file, rightPageNo, tempPage);
    T_LeafNode* rightPage = reinterpret_cast<T_LeafNode*>(tempPage);

    T parentKey = rightPage->keyArray[0];
    bufMgr->unPinPage(file, PageNo, false);
    bufMgr->unPinPage(file, rightPageNo, false);

    // decide which node to insert the new key 
    if ( thisKey < parentKey ) {
      // insert into current node
      insertLeafNode(pageNo, rkpair);
    } else {
      // insert into right sibling node
      insertLeafNode(rightPageNo, rkpair);
    }

  }
}

// insertLeafNode<int, struct LeafNodeInt>(rootPageNum, rkpair);


// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------
template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::splitLeafNode(PageId pageNo)
{
    Page *tempPage; // can be reused
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file, firstPageNo, tempPage);
    T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
    PageId secondPageNo;
    bufMgr->allocPage(file, secondPageNo, tempPage);
    T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

    // set the parent properly
    secondPage->parentPageNo = firstPage->parentPageNo;

    // set the sibling properly
    secondPage->rightSibPageNo = firstPage->rightSibPageNo;
    firstPage->rightSibPageNo = secondPageNo;

    // middle one, as the new key in parent
    // firstPage has more keys if odd size
    // copy the second half keys to secondPage
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    for ( int i1 = midIndex, i2=0 ; i1 < leafOccupancy ; ++i1,++i2) {
      secondPage->keyArray[i2] = firstPage->keyArray[i1];
      secondPage->ridArray[i2] = firstPage->ridArray[i1];
    }
    firstPage->size = midIndex;
    secondPage->size = leafOccupancy - midIndex;

    T copyUpKey = firstPage->keyArray[midIndex];

    // check if here is parent
    //
    // I think, as long as the parent page no is not -1, it is not root 
    // but we can do extra check, TODO
    if ( firstPage->parentPageNo > 1 ) { // normal parent, insert new pkpair
      PageKeyPair<T> pkpair;
      pkpair.set(secondPageNo, copyUpKey);
      insertNonLeafNode(firstPage->parentPageNo, pkpair);
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

      // construct the parent node
      parentPage->level = 1;  // just above leaf
      parentPage->size = 2;   // first and second page
      parentPage->keyArray[0] = copyUpKey;
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;

      // done with parentPage, save changes
      bufMgr->unPinPage(file, parentPageNo, true);
    }


}


template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::insertNonLeafNode(PageId pageNo, PageKeyPair<T> pkpair) {

    Page *tempPage;
    bufMgr->readPage(file, pageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    T thisKey = pkpair->key;
    int size = thisPage->size;
    if ( size < nodeOccupancy ) {
      // still have space, just insert

      int index = 0;
      // find the correct position to insert the key
      while ( thisKey > thisPage->keyArray[index] ) ++index;
      // shift all keys and pageNos right by 1 position
      for ( int i = size ; i > index ; --i ) {
        thisPage->keyArray[i] = thisPage->keyArray[i-1];
        thisPage->pageNoArray[i+1] = thisPage->pageNoArray[i];
      }
      // insert the current key
      thisPage->keyArray[index] = thisKey;
      thisPage->pageNoArray[index+1] = pkpair.pageNo;
    } else {
      // need to split this non-leaf node, potentially propagate up
      // TODO split the non leaf node

    }
    
}



template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::splitNonLeafNode(PageId pageNo)
{
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode * thisNode = reinterpret_cast<T_NonLeafNode*>(tempPage);

  if ( thisNode->size < nodeOccupancy ) {
    // no split needed, done
  } else {
  }

}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  if ( scanExecuting == true ) {
    // error, only one scan at a time
    std::cout<< "Scan Already started\n";
  } else {
    scanExecuting = true;
  }


//   /**
//    * True if an index scan has been started.
//    */
// 	bool		scanExecuting;
// 
//   /**
//    * Index of next entry to be scanned in current leaf being scanned.
//    */
// 	int		nextEntry;
// 
//   /**
//    * Page number of current page being scanned.
//    */
// 	PageId	currentPageNum;
// 
//   /**
//    * Current Page being scanned.
//    */
// 	Page		*currentPageData;
// 
//   /**
//    * Low INTEGER value for scan.
//    */
// 	int		lowValInt;
// 
//   /**
//    * Low DOUBLE value for scan.
//    */
// 	double	lowValDouble;
// 
//   /**
//    * Low STRING value for scan.
//    */
// 	std::string	lowValString;
// 
//   /**
//    * High INTEGER value for scan.
//    */
// 	int		highValInt;
// 
//   /**
//    * High DOUBLE value for scan.
//    */
// 	double	highValDouble;
// 
//   /**
//    * High STRING value for scan.
//    */
// 	std::string highValString;
// 	
//   /**
//    * Low Operator. Can only be GT(>) or GTE(>=).
//    */
// 	Operator	lowOp;
// 
//   /**
//    * High Operator. Can only be LT(<) or LTE(<=).
//    */
// 	Operator	highOp;






}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
  // make sure the scan state is true;
  if ( scanExecuting == false) {
    std::cout<<"No scann started\n";
    return;
  }



  // if scan reach the end of the list
//   throw IndexScanCompletedException;
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

  // end the scan, unpin all pages have been pinned
  //

}

}


